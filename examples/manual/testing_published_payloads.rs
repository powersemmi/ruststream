//! The payload assertions over what was published to one destination, against services written
//! without the `macros` feature. The test bodies are the ones of
//! `examples/testing_published_payloads.rs`: the harness reads the same records whichever way the
//! service was declared.
//!
//! ```text
//! cargo test --example manual_testing_published_payloads --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::codec::{Codec, CodecError, JsonCodec};
use ruststream::memory::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
    seats: u32,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
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

/// A receipt for the legacy system, sent wherever the mount site says.
#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Invoice {
    order_id: u64,
}

impl OutgoingDestination for Invoice {
    type Form = CallerName;
}

impl MessageHeaders for Invoice {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Ticket {
    order_id: u64,
    seat: u32,
}

impl OutgoingDestination for Ticket {
    type Form = CallerName;
}

impl MessageHeaders for Ticket {
    type Contract = NoHeaders;
}

impl<M: OutSlot> OutMessages<M> for Ticket {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        Vec::new()
    }
}

/// The service's own wire format: JSON behind a one-byte format version.
#[derive(Debug, Clone, Copy, Default)]
struct Versioned;

impl Codec for Versioned {
    fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
        let mut framed = BytesMut::from(&[1_u8][..]);
        JsonCodec.encode_into(value, &mut framed)?;
        Ok(framed)
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        JsonCodec.decode(bytes.get(1..).unwrap_or_default())
    }
}

mod decoded {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Invoice, Order, Receipt, Versioned};

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

    struct IssueInvoice;

    impl Handle<Order, Invoice> for IssueInvoice {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Invoice, HandlerOutcome>> {
            ready(Ok(Invoice { order_id: order.id }))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", IssueReceipt).reply().build())
                .out_reply(Publish);
            b.include(
                subscriber("legacy-orders", IssueInvoice)
                    .reply()
                    .to("invoices")
                    .build(),
            )
            .out_reply(Publish)
            .codec(Versioned);
        })
    }

    // --8<-- [start:decoded]
    #[tokio::test]
    async fn every_order_got_its_receipt_in_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        for id in [7, 8] {
            broker
                .message(&Order { id, seats: 1 })
                .to("orders")
                .publish()
                .await?;
        }
        broker
            .message(&Order { id: 9, seats: 1 })
            .to("legacy-orders")
            .publish()
            .await?;

        let receipts = broker.published::<Receipt>("receipts").decoded();
        assert_eq!(receipts, [Receipt { order_id: 7 }, Receipt { order_id: 8 }]);
        let invoices = broker
            .published::<Invoice>("invoices")
            .decoded_with(&Versioned);
        assert_eq!(invoices, [Invoice { order_id: 9 }]);
        Ok(())
    }
    // --8<-- [end:decoded]
}

mod decoded_as {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Ticket};

    struct Tickets;

    impl OutSlot for Tickets {
        const NAME: &'static str = "Tickets";
        type Destination = Reads;

        fn outgoing() -> Vec<OutgoingMessageMetadata> {
            <Ticket as OutMessages<Self>>::outgoing()
        }
    }

    impl PublishedThrough<Tickets> for Ticket {}

    struct IssueTickets;

    impl<Entry> Handle<Order, (), Outs<(Entry,)>> for IssueTickets
    where
        Entry: OutEntry<Tickets, Wire: Publisher>,
    {
        async fn handle(
            &self,
            order: &Order,
            outs: &Outs<(Entry,)>,
            _ctx: &mut Context<'_>,
        ) -> Result<(), HandlerOutcome> {
            for seat in 1..=order.seats {
                let ticket = Ticket {
                    order_id: order.id,
                    seat,
                };
                let sent = outs
                    .get(Tickets)
                    .message(&ticket)
                    .to("tickets")
                    .publish()
                    .await;
                if sent.is_err() {
                    return Err(HandlerOutcome::retry());
                }
            }
            Ok(())
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", IssueTickets).build())
                .out(Tickets, Publish)
                .build();
        })
    }

    // --8<-- [start:decoded-as]
    #[tokio::test]
    async fn the_slot_sent_the_last_seat_last() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MemoryBroker>()
            .message(&Order { id: 7, seats: 2 })
            .to("orders")
            .publish()
            .await?;

        tb.out::<Tickets>()
            .assert_called(2)
            .decoded_as::<Ticket>()
            .with(&Ticket {
                order_id: 7,
                seat: 2,
            });
        Ok(())
    }
    // --8<-- [end:decoded-as]
}

mod with {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Invoice, Order, Receipt, Versioned};

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

    struct IssueInvoice;

    impl Handle<Order, Invoice> for IssueInvoice {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Invoice, HandlerOutcome>> {
            ready(Ok(Invoice { order_id: order.id }))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", IssueReceipt).reply().build())
                .out_reply(Publish);
            b.include(
                subscriber("legacy-orders", IssueInvoice)
                    .reply()
                    .to("invoices")
                    .build(),
            )
            .out_reply(Publish)
            .codec(Versioned);
        })
    }

    // --8<-- [start:with]
    #[tokio::test]
    async fn the_receipt_names_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        for queue in ["orders", "legacy-orders"] {
            broker
                .message(&Order { id: 7, seats: 1 })
                .to(queue)
                .publish()
                .await?;
        }

        broker
            .published::<Receipt>("receipts")
            .with(&Receipt { order_id: 7 });
        broker
            .published::<Invoice>("invoices")
            .with_codec(&Versioned, &Invoice { order_id: 7 });
        Ok(())
    }
    // --8<-- [end:with]
}

mod with_raw {
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
                .out_reply(Publish);
        })
    }

    // --8<-- [start:with-raw]
    #[tokio::test]
    async fn the_receipt_keeps_its_wire_form() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7, seats: 1 })
            .to("orders")
            .publish()
            .await?;

        broker
            .published::<Receipt>("receipts")
            .with_raw(br#"{"order_id":7}"#);
        Ok(())
    }
    // --8<-- [end:with-raw]
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
