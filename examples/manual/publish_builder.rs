//! The service of `examples/publish_builder.rs` written without the `macros` feature. The
//! builder is the same in both forms: the slot's entry offers it.
//!
//! ```text
//! cargo test --example manual_publish_builder --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use ruststream::runtime::PublishError;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Shipment {
    id: u64,
    region: String,
    carrier: String,
}

impl OutgoingDestination for Shipment {
    type Form = CallerName;
}

impl MessageHeaders for Shipment {
    type Contract = NoHeaders;
}

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

// --8<-- [start:handler]
struct Notify;

impl<Egress> Handle<Shipment, (), Outs<(Egress,)>> for Notify
where
    Egress: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        shipment: &Shipment,
        outs: &Outs<(Egress,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let mut headers = HeaderMap::new();
        headers.insert("carrier", shipment.carrier.clone());
        let sent = outs
            .get(DefaultSlot)
            .message(&Notice { id: shipment.id })
            .to(format!("notices.{}", shipment.region))
            .with_headers(headers)
            .publish()
            .await;
        match sent {
            Ok(()) => Ok(()),
            // A value that does not encode fails the same way on every attempt.
            Err(PublishError::Encode(_) | PublishError::Serialize(_)) => {
                Err(HandlerOutcome::drop())
            }
            Err(_) => Err(HandlerOutcome::retry()),
        }
    }
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("shipments", Notify).build())
            .out(DefaultSlot, Publish)
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
