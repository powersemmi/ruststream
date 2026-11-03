//! Destinations: a message type declares where it goes, as a fixed name, a name template, or
//! nothing, which leaves the name to the call. The scenarios are tests.
//!
//! ```text
//! cargo test --example destinations --features testing,macros,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    tenant: String,
}

// --8<-- [start:messages]
/// A fixed name: every publish of this type goes to `orders.confirmed`.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.confirmed")]
struct OrderConfirmed {
    id: u64,
}

/// A name template: each placeholder becomes a setter on the publish builder.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.{tenant}.placed")]
struct OrderPlaced {
    id: u64,
}

/// No name: the call site names the destination.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Notice {
    id: u64,
}
// --8<-- [end:messages]

#[derive(OutSlot)]
#[publishes(OrderConfirmed, OrderPlaced, Notice)]
struct Orders;

// --8<-- [start:handler]
#[subscriber("orders.incoming")]
async fn route(order: &Order, Out(orders): Out<impl Publisher, Orders>) -> HandlerOutcome {
    let confirmed = orders
        .message(&OrderConfirmed { id: order.id })
        .publish()
        .await;
    let placed = orders
        .message(&OrderPlaced { id: order.id })
        .to()
        .tenant(&order.tenant)
        .publish()
        .await;
    let notice = orders
        .message(&Notice { id: order.id })
        .to("notices")
        .publish()
        .await;
    if confirmed.is_err() || placed.is_err() || notice.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(route).out(Orders, Publish)
    })
}

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Notice, Order, OrderConfirmed, OrderPlaced, app};

    #[tokio::test]
    async fn each_message_goes_where_its_type_says() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order {
                id: 8,
                tenant: "acme".into(),
            })
            .to("orders.incoming")
            .publish()
            .await?;

        broker
            .published::<OrderConfirmed>("orders.confirmed")
            .assert_called_once()
            .with(&OrderConfirmed { id: 8 });
        broker
            .published::<OrderPlaced>("orders.acme.placed")
            .assert_called_once()
            .with(&OrderPlaced { id: 8 });
        broker
            .published::<Notice>("notices")
            .assert_called_once()
            .with(&Notice { id: 8 });
        Ok(())
    }
}
