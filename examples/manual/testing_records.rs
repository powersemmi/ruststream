//! The records of the test client, against services written without the `macros` feature. The
//! test bodies are the ones of `examples/testing_records.rs`: the harness reads the same records
//! whichever way the service was declared.
//!
//! ```text
//! cargo test --example manual_testing_records --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

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

impl<M: OutSlot> OutMessages<M> for Order {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        Vec::new()
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Receipt {
    order_id: u64,
}

// `#[outgoing(name = "receipts")]` by hand: the type fixes its destination.
impl OutgoingDestination for Receipt {
    type Form = FixedName;

    const DESTINATION: &'static str = "receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

mod subscriber {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::Order;

    struct Accept;

    impl Handle<Order> for Accept {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            if order.id == 0 {
                return ready(Err(HandlerOutcome::drop()));
            }
            ready(Ok(()))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Accept).build())
        })
    }

    // --8<-- [start:subscriber]
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
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Receipt};

    struct IssueReceipt;

    impl Handle<Order, Receipt> for IssueReceipt {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
            ready(Ok(Receipt { order_id: order.id }))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", IssueReceipt).reply().build())
                .out_reply(Publish)
        })
    }

    // --8<-- [start:published]
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

    struct Audit;

    impl OutSlot for Audit {
        const NAME: &'static str = "Audit";
        type Destination = Reads;

        fn outgoing() -> Vec<OutgoingMessageMetadata> {
            <Order as OutMessages<Self>>::outgoing()
        }
    }

    impl PublishedThrough<Audit> for Order {}

    struct CopyToAudit;

    impl<Entry> Handle<Order, (), Outs<(Entry,)>> for CopyToAudit
    where
        Entry: OutEntry<Audit, Wire: Publisher>,
    {
        async fn handle(
            &self,
            order: &Order,
            outs: &Outs<(Entry,)>,
            _ctx: &mut Context<'_>,
        ) -> Result<(), HandlerOutcome> {
            if outs
                .get(Audit)
                .message(order)
                .to("audit")
                .publish()
                .await
                .is_err()
            {
                return Err(HandlerOutcome::retry());
            }
            Ok(())
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", CopyToAudit).build())
                .out(Audit, Publish)
        })
    }

    // --8<-- [start:out]
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

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
