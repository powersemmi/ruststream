//! The service of `examples/destinations.rs` written without the `macros` feature: each message
//! type states its destination form by hand, and a name template names the builder of its
//! setters.
//!
//! ```text
//! cargo test --example manual_destinations --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::fmt::Display;

use ruststream::memory::prelude::*;
use ruststream::runtime::{BoundSegment, MissingSegment, PublishAt, PublishError, TemplateAddress};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
    tenant: String,
}

impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

/// The `tenant` placeholder, named in the compile error of a publish that never bound it.
#[derive(Debug, Clone, Copy)]
struct Tenant;

/// The setters of `orders.{tenant}.placed`: `publish` exists once every placeholder is bound.
#[must_use = "an address builder does nothing until publish() is awaited"]
struct PlacedAt<Cont, S0> {
    cont: Cont,
    tenant: S0,
}

impl<Cont> PlacedAt<Cont, MissingSegment<Tenant>> {
    fn tenant(self, value: impl Display) -> PlacedAt<Cont, String> {
        PlacedAt {
            cont: self.cont,
            tenant: value.to_string(),
        }
    }
}

impl<Cont, S0> PlacedAt<Cont, S0> {
    async fn publish(self) -> Result<(), PublishError<<Cont as PublishAt>::Error>>
    where
        Cont: PublishAt,
        S0: BoundSegment,
    {
        let address = format!("orders.{}.placed", self.tenant);
        PublishAt::publish_at(self.cont, &address).await
    }
}

// --8<-- [start:messages]
/// A fixed name: every publish of this type goes to `orders.confirmed`.
#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct OrderConfirmed {
    id: u64,
}

impl OutgoingDestination for OrderConfirmed {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders.confirmed";
}

impl MessageHeaders for OrderConfirmed {
    type Contract = NoHeaders;
}

/// A name template: the type names the builder that carries one setter per placeholder.
#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct OrderPlaced {
    id: u64,
}

impl OutgoingDestination for OrderPlaced {
    type Form = NameTemplate;

    const DESTINATION: &'static str = "orders.{tenant}.placed";
    const PARAMETERS: &'static [&'static str] = &["tenant"];
}

impl MessageHeaders for OrderPlaced {
    type Contract = NoHeaders;
}

impl<Cont> TemplateAddress<Cont> for OrderPlaced {
    type Builder = PlacedAt<Cont, MissingSegment<Tenant>>;

    fn begin(cont: Cont) -> Self::Builder {
        PlacedAt {
            cont,
            tenant: MissingSegment::new(),
        }
    }
}

/// No name: the call site names the destination.
#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Notice {
    id: u64,
}

impl OutgoingDestination for Notice {
    type Form = CallerName;
}

impl MessageHeaders for Notice {
    type Contract = NoHeaders;
}
// --8<-- [end:messages]

struct Orders;

impl OutSlot for Orders {
    const NAME: &'static str = "Orders";
    type Destination = Reads;
}

impl PublishedThrough<Orders> for OrderConfirmed {}
impl PublishedThrough<Orders> for OrderPlaced {}
impl PublishedThrough<Orders> for Notice {}

// --8<-- [start:handler]
struct Route;

impl<O> Handle<Order, (), Outs<(O,)>> for Route
where
    O: OutEntry<Orders, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(O,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let orders = outs.get(Orders);
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
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders.incoming", Route).build())
            .out(Orders, Publish)
            .build();
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
