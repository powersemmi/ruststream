//! The records of the test client: what a handler received, what was published to a destination,
//! and what left through one `Out` slot. Every scenario is a test of its own, the way a service
//! writes it.
//!
//! ```text
//! cargo test --example testing_records --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

mod subscriber {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:subscriber]
    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        if order.id == 0 {
            return HandlerOutcome::drop();
        }
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
        })
    }

    #[tokio::test]
    async fn the_handler_received_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let orders = tb.broker::<MemoryBroker>();
        orders
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        orders
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
    // --8<-- [end:subscriber]
}

mod published {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Receipt};

    // --8<-- [start:published]
    #[subscriber("orders", reply)]
    async fn issue_receipt(order: &Order) -> Receipt {
        Receipt { order_id: order.id }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(issue_receipt).out_reply(Publish);
        })
    }

    #[tokio::test]
    async fn a_receipt_is_published() -> Result<(), Box<dyn Error>> {
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
        Ok(())
    }
    // --8<-- [end:published]
}

mod out {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    // --8<-- [start:out]
    #[derive(OutSlot)]
    #[publishes(Order)]
    struct Audit;

    #[subscriber("orders")]
    async fn copy_to_audit(order: &Order, Out(out): Out<impl Publisher, Audit>) -> HandlerOutcome {
        if out.message(order).to("audit").publish().await.is_err() {
            return HandlerOutcome::retry();
        }
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(copy_to_audit).out(Audit, Publish).build();
        })
    }

    #[tokio::test]
    async fn the_audit_slot_carried_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MemoryBroker>()
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        let copies = tb.out::<Audit>().assert_called_once();
        assert_eq!(copies.messages()[0].name(), "audit");
        assert_eq!(copies.decoded_as::<Order>().decoded(), [Order { id: 7 }]);
        Ok(())
    }
    // --8<-- [end:out]
}

fn main() {
    println!("the scenarios are tests: cargo test --example testing_records");
}
