//! `Out` slots: a handler takes a live publisher as a parameter, one marker per slot, and the
//! mount site binds each marker to a publish policy. The scenarios are tests.
//!
//! ```text
//! cargo test --example out_slots --features testing,macros,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Event {
    id: u64,
}

// --8<-- [start:forward]
/// One unnamed slot: the publisher arrives live, and `Event` leaves the destination to the call.
#[subscriber("ingress")]
async fn forward(event: &Event, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    if out.message(event).to("egress").publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:forward]

// --8<-- [start:slots]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.confirmed")]
struct Confirmed {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Audit {
    id: u64,
}

/// A slot marker lists what may leave through the slot.
#[derive(OutSlot)]
#[publishes(Confirmed)]
struct Orders;

#[derive(OutSlot)]
#[publishes(Audit)]
struct Trail;

#[subscriber("orders.incoming")]
async fn confirm(
    event: &Event,
    Out(orders): Out<impl Publisher, Orders>,
    Out(trail): Out<impl Publisher, Trail>,
) -> HandlerOutcome {
    if orders
        .message(&Confirmed { id: event.id })
        .publish()
        .await
        .is_err()
        || trail
            .message(&Audit { id: event.id })
            .to("audit")
            .publish()
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:slots]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(forward).out(DefaultSlot, Publish).build();
        // Each marker binds its own policy, in any order.
        b.include(confirm)
            .out(Orders, Publish)
            .out(Trail, Publish)
            .build();
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
