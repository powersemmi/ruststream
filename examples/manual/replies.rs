//! The service of `examples/replies.rs` written without the `macros` feature: a reply is the
//! second axis of `Handle`, and the chain says where it goes when the type does not.
//!
//! ```text
//! cargo test --example manual_replies --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:declared]
/// The reply type fixes its destination, so the chain names none.
#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Receipt {
    order_id: u64,
}

// `#[outgoing(name = "receipts")]` by hand.
impl OutgoingDestination for Receipt {
    type Form = FixedName;

    const DESTINATION: &'static str = "receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

/// `Ok` publishes the receipt and acks; `Err` publishes nothing and settles by the outcome.
struct IssueReceipt;

impl Handle<Order, Receipt> for IssueReceipt {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        if order.id == 0 {
            return ready(Err(HandlerOutcome::drop()));
        }
        ready(Ok(Receipt { order_id: order.id }))
    }
}
// --8<-- [end:declared]

// --8<-- [start:named]
/// The reply type declares no destination, so the chain names one.
#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Quote {
    order_id: u64,
    price: u64,
}

impl OutgoingDestination for Quote {
    type Form = CallerName;
}

impl MessageHeaders for Quote {
    type Contract = NoHeaders;
}

struct QuoteOrder;

impl Handle<Order, Quote> for QuoteOrder {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Quote, HandlerOutcome>> {
        ready(Ok(Quote {
            order_id: order.id,
            price: order.id * 10,
        }))
    }
}
// --8<-- [end:named]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // The broker's default policy, under the default codec.
        b.include(subscriber("orders", IssueReceipt).reply().build());
        // The destination the type leaves open, and the policy the reply leaves through.
        b.include(
            subscriber("quote-requests", QuoteOrder)
                .reply()
                .to("quotes")
                .build(),
        )
        .out_reply(Publish);
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
