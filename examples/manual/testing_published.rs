//! The assertions over what was published to one destination, against services written without
//! the `macros` feature. The test bodies are the ones of `examples/testing_published.rs`: the
//! harness reads the same records whichever way the service was declared.
//!
//! ```text
//! cargo test --example manual_testing_published --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
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

mod assert_called_once {
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
            if order.seats == 0 {
                return ready(Err(HandlerOutcome::drop()));
            }
            ready(Ok(Receipt { order_id: order.id }))
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", IssueReceipt).reply().build())
                .out_reply(Publish)
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
                let mut headers = HeaderMap::new();
                headers.insert("x-order", order.id.to_string());
                let sent = outs
                    .get(Tickets)
                    .message(&ticket)
                    .to("tickets")
                    .with_headers(headers)
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
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::runtime::{Outgoing, PublishLayer, PublishNext, PublishPipeline};
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
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .publish_layer(ServiceStamp)
            .with_broker(MemoryBroker::new(), |b| {
                b.include(subscriber("orders", IssueReceipt).reply().build())
                    .out_reply(Publish)
                    .transform(SchemaVersion)
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
