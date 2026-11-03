//! The publish builder: `message(&value)`, the positions the type leaves open, then `publish()`.
//! The scenarios are tests.
//!
//! ```text
//! cargo test --example publish_builder --features testing,macros,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use ruststream::runtime::PublishError;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Shipment {
    id: u64,
    region: String,
    carrier: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Notice {
    id: u64,
}

// --8<-- [start:handler]
#[subscriber("shipments")]
async fn notify(shipment: &Shipment, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    headers.insert("carrier", shipment.carrier.clone());
    let sent = out
        .message(&Notice { id: shipment.id })
        .to(format!("notices.{}", shipment.region))
        .with_headers(headers)
        .publish()
        .await;
    match sent {
        Ok(()) => HandlerOutcome::ack(),
        // A value that does not encode fails the same way on every attempt.
        Err(PublishError::Encode(_) | PublishError::Serialize(_)) => HandlerOutcome::drop(),
        Err(_) => HandlerOutcome::retry(),
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(notify).out(DefaultSlot, Publish)
    })
}

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Notice, Shipment, app};

    #[tokio::test]
    async fn the_notice_goes_to_its_region_with_the_carrier() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Shipment {
                id: 2,
                region: "eu".into(),
                carrier: "dhl".into(),
            })
            .to("shipments")
            .publish()
            .await?;

        broker
            .published::<Notice>("notices.eu")
            .assert_called_once()
            .with(&Notice { id: 2 })
            .with_header("carrier", "dhl");
        broker
            .subscriber("shipments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
