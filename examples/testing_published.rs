//! The assertions over what was published to one destination: how many messages, and what each
//! carried in its headers. Every scenario is a test of its own, the way a service writes it.
//!
//! ```text
//! cargo test --example testing_published --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
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

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Ticket {
    order_id: u64,
    seat: u32,
}

mod assert_called_once {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Receipt};

    #[subscriber("orders", reply)]
    async fn issue_receipt(order: &Order) -> Receipt {
        Receipt { order_id: order.id }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(issue_receipt).out_reply(Publish);
        })
    }

    // --8<-- [start:assert-called-once]
    #[tokio::test]
    async fn one_order_gets_one_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7, seats: 2 })
            .to("orders")
            .publish()
            .await?;

        broker.published::<Receipt>("receipts").assert_called_once();
        Ok(())
    }
    // --8<-- [end:assert-called-once]
}

mod assert_called {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Ticket};

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

    // --8<-- [start:assert-called]
    #[tokio::test]
    async fn one_ticket_per_seat() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7, seats: 3 })
            .to("orders")
            .publish()
            .await?;

        broker.subscriber("orders").assert_called_once();
        broker.published::<Ticket>("tickets").assert_called(3);
        Ok(())
    }
    // --8<-- [end:assert-called]
}

mod assert_not_called {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Receipt};

    #[subscriber("orders", reply)]
    async fn issue_receipt(order: &Order) -> Result<Receipt, HandlerOutcome> {
        if order.seats == 0 {
            return Err(HandlerOutcome::drop());
        }
        Ok(Receipt { order_id: order.id })
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(issue_receipt).out_reply(Publish);
        })
    }

    // --8<-- [start:assert-not-called]
    #[tokio::test]
    async fn an_empty_order_gets_no_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7, seats: 0 })
            .to("orders")
            .publish()
            .await?;

        broker.published::<Receipt>("receipts").assert_not_called();
        Ok(())
    }
    // --8<-- [end:assert-not-called]
}

mod messages {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Ticket};

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
            let mut headers = HeaderMap::new();
            headers.insert("x-order", order.id.to_string());
            let sent = out
                .message(&ticket)
                .to("tickets")
                .with_headers(headers)
                .publish()
                .await;
            if sent.is_err() {
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

    // --8<-- [start:messages]
    #[tokio::test]
    async fn every_ticket_names_its_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7, seats: 2 })
            .to("orders")
            .publish()
            .await?;

        let tickets = broker.published::<Ticket>("tickets");
        assert_eq!(tickets.messages().len(), 2);
        for ticket in tickets.messages() {
            assert_eq!(ticket.name(), "tickets");
            assert_eq!(ticket.headers().get("x-order"), Some(&b"7"[..]));
        }
        Ok(())
    }
    // --8<-- [end:messages]
}

mod with_header {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::runtime::{
        ContextKind, Outgoing, PublishLayer, PublishNext, PublishPipeline, PublishTransform, Reads,
    };
    use ruststream::testing::TestApp;

    use super::{Order, Receipt};

    /// Stamps the schema version on every reply this mounting sends.
    struct SchemaVersion;

    impl<K: ContextKind, Options> PublishTransform<K, Options> for SchemaVersion {
        type Destination = Reads;

        fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &K::View<'_>) {
            out.headers_mut().insert("x-schema", "2");
        }
    }

    /// Names the service on everything it publishes.
    #[derive(Clone)]
    struct ServiceStamp;

    impl PublishLayer for ServiceStamp {
        async fn on_publish<'a, N: PublishPipeline, P: Publisher>(
            &'a self,
            out: &'a mut Outgoing<'a>,
            next: PublishNext<'a, N, P>,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            out.headers_mut().insert("x-service", "orders");
            next.run(out).await
        }
    }

    #[subscriber("orders", reply)]
    async fn issue_receipt(order: &Order) -> Receipt {
        Receipt { order_id: order.id }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .publish_layer(ServiceStamp)
            .with_broker(MemoryBroker::new(), |b| {
                b.include(issue_receipt)
                    .out_reply(Publish)
                    .transform(SchemaVersion);
            })
    }

    // --8<-- [start:with-header]
    #[tokio::test]
    async fn the_receipt_left_stamped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7, seats: 1 })
            .to("orders")
            .publish()
            .await?;

        broker
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with_header("x-schema", "2")
            .with_header("x-service", "orders");
        Ok(())
    }
    // --8<-- [end:with-header]
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
