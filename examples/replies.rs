//! Replies: a handler answers a message by returning a value, and the reply type or the mount
//! site says where the answer goes. The scenarios are tests.
//!
//! ```text
//! cargo test --example replies --features testing,macros,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:declared]
/// The reply type fixes its destination, so the clause is the bare `reply`.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

/// `Ok` publishes the receipt and acks; `Err` publishes nothing and settles by the outcome.
#[subscriber("orders", reply)]
async fn issue_receipt(order: &Order) -> Result<Receipt, HandlerOutcome> {
    if order.id == 0 {
        return Err(HandlerOutcome::drop());
    }
    Ok(Receipt { order_id: order.id })
}
// --8<-- [end:declared]

// --8<-- [start:named]
/// The reply type declares no destination, so the clause names one.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Quote {
    order_id: u64,
    price: u64,
}

#[subscriber("quote-requests", reply("quotes"))]
async fn quote(order: &Order) -> Quote {
    Quote {
        order_id: order.id,
        price: order.id * 10,
    }
}
// --8<-- [end:named]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // The broker's default policy, under the default codec.
        b.include(issue_receipt)
            // The policy the reply leaves through, named at the mount site.
            .include(quote)
            .out_reply(Publish)
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

    use super::{Order, Quote, Receipt, app};

    #[tokio::test]
    async fn an_order_gets_its_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        broker
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 7 });
        broker
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn an_order_without_an_id_gets_no_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 0 })
            .to("orders")
            .publish()
            .await?;

        broker.published::<Receipt>("receipts").assert_not_called();
        broker
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        Ok(())
    }

    #[tokio::test]
    async fn a_quote_goes_where_the_mount_site_says() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 4 })
            .to("quote-requests")
            .publish()
            .await?;

        broker
            .published::<Quote>("quotes")
            .assert_called_once()
            .with(&Quote {
                order_id: 4,
                price: 40,
            });
        Ok(())
    }
}
