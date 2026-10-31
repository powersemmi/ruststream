//! The service of `examples/out_slots.rs` written without the `macros` feature: a slot is one
//! entry of the body's `Outs`, named by its marker, and the mount site binds the same markers.
//!
//! ```text
//! cargo test --example manual_out_slots --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Event {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Event {
    type Form = CallerName;
}

impl MessageHeaders for Event {
    type Contract = NoHeaders;
}

// --8<-- [start:forward]
/// One unnamed slot: the entry arrives live, and `Event` leaves the destination to the call.
struct Forward;

impl<Egress> Handle<Event, (), Outs<(Egress,)>> for Forward
where
    Egress: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        event: &Event,
        outs: &Outs<(Egress,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if outs
            .get(DefaultSlot)
            .message(event)
            .to("egress")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:forward]

// --8<-- [start:slots]
#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Confirmed {
    id: u64,
}

impl OutgoingDestination for Confirmed {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders.confirmed";
}

impl MessageHeaders for Confirmed {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Audit {
    id: u64,
}

impl OutgoingDestination for Audit {
    type Form = CallerName;
}

impl MessageHeaders for Audit {
    type Contract = NoHeaders;
}

/// A slot marker lists what may leave through the slot.
struct Orders;

impl OutSlot for Orders {
    const NAME: &'static str = "Orders";
    // `Confirmed` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Orders> for Confirmed {}

struct Trail;

impl OutSlot for Trail {
    const NAME: &'static str = "Trail";
    // `Audit` leaves its destination open, so a transform on this slot may name it.
    type Destination = Names;
}

impl PublishedThrough<Trail> for Audit {}

struct Confirm;

impl<O, T> Handle<Event, (), Outs<(O, T)>> for Confirm
where
    O: OutEntry<Orders, Wire: Publisher>,
    T: OutEntry<Trail, Wire: Publisher>,
{
    async fn handle(
        &self,
        event: &Event,
        outs: &Outs<(O, T)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if outs
            .get(Orders)
            .message(&Confirmed { id: event.id })
            .publish()
            .await
            .is_err()
            || outs
                .get(Trail)
                .message(&Audit { id: event.id })
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
// --8<-- [end:slots]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("ingress", Forward).build())
            .out(DefaultSlot, Publish)
            // Each marker binds its own policy, in any order.
            .include(subscriber("orders.incoming", Confirm).build())
            .out(Orders, Publish)
            .out(Trail, Publish)
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

    use super::{Audit, Confirmed, Event, Trail, app};

    #[tokio::test]
    async fn an_event_is_forwarded() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Event { id: 3 })
            .to("ingress")
            .publish()
            .await?;

        broker
            .published::<Event>("egress")
            .assert_called_once()
            .with(&Event { id: 3 });
        Ok(())
    }

    #[tokio::test]
    async fn each_slot_publishes_its_own_message() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Event { id: 5 })
            .to("orders.incoming")
            .publish()
            .await?;

        broker
            .published::<Confirmed>("orders.confirmed")
            .assert_called_once()
            .with(&Confirmed { id: 5 });
        tb.out::<Trail>()
            .assert_called_once()
            .decoded_as::<Audit>()
            .with(&Audit { id: 5 });
        Ok(())
    }
}
