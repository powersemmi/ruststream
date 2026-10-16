//! The payload assertions over what was published to one destination: every message decoded, the
//! latest one decoded or compared byte for byte, under the default codec or the mount's own.
//! Every scenario is a test of its own, the way a service writes it.
//!
//! ```text
//! cargo test --example testing_published_payloads --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::BytesMut;
use ruststream::codec::{Codec, CodecError, JsonCodec};
use ruststream::memory::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    seats: u32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

/// A receipt for the legacy system, sent wherever the mount site says.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Invoice {
    order_id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Ticket {
    order_id: u64,
    seat: u32,
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

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Invoice, Order, Receipt, Versioned};

    // --8<-- [start:decoded]
    #[subscriber("orders", reply)]
    async fn issue_receipt(order: &Order) -> Receipt {
        Receipt { order_id: order.id }
    }

    #[subscriber("legacy-orders", reply("invoices"))]
    async fn issue_invoice(order: &Order) -> Invoice {
        Invoice { order_id: order.id }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(issue_receipt).out_reply(Publish);
            b.include(issue_invoice).out_reply(Publish).codec(Versioned);
        })
    }

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

    // --8<-- [start:decoded-as]
    #[derive(OutSlot)]
    #[publishes(Ticket)]
    struct Tickets;

    #[subscriber("orders")]
    async fn issue_tickets(
        order: &Order,
        Out(out): Out<impl Publisher, Tickets>,
    ) -> HandlerOutcome {
        for seat in 1..=order.seats {
            let ticket = Ticket {
                order_id: order.id,
                seat,
            };
            if out.message(&ticket).to("tickets").publish().await.is_err() {
                return HandlerOutcome::retry();
            }
        }
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(issue_tickets).out(Tickets, Publish).build();
        })
    }

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

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Invoice, Order, Receipt, Versioned};

    // --8<-- [start:with]
    #[subscriber("orders", reply)]
    async fn issue_receipt(order: &Order) -> Receipt {
        Receipt { order_id: order.id }
    }

    #[subscriber("legacy-orders", reply("invoices"))]
    async fn issue_invoice(order: &Order) -> Invoice {
        Invoice { order_id: order.id }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(issue_receipt).out_reply(Publish);
            b.include(issue_invoice).out_reply(Publish).codec(Versioned);
        })
    }

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

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Receipt};

    // --8<-- [start:with-raw]
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
