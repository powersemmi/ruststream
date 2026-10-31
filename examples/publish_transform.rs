//! Publish transforms: a static step on one publish position, which reads the delivery (a reply)
//! or the slot's name (an `Out` slot) and changes the outgoing message. The scenarios are tests.
//!
//! ```text
//! cargo test --example publish_transform --features testing,macros,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use ruststream::runtime::{Outgoing, PublishContext, SlotContext};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Receipt {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Payment {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Audit {
    id: u64,
}

// --8<-- [start:reply]
/// Carries the request's correlation id onto its reply. `ForReply` reads the delivery being
/// answered, so this transform mounts on a reply position.
struct Correlate;

impl<C, Options> PublishTransform<ForReply<C>, Options> for Correlate {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        // The delivery's own buffer, shared rather than copied.
        if let Some(id) = cx.headers().get_shared("correlation-id") {
            out.headers_mut().insert("correlation-id", id);
        }
    }
}
// --8<-- [end:reply]

// --8<-- [start:slot]
/// Stamps the schema version on what leaves a slot. `ForSlot` reads the slot's name and nothing
/// else: the body that publishes has already read its delivery.
struct SchemaVersion(&'static str);

impl<Options> PublishTransform<ForSlot, Options> for SchemaVersion {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &SlotContext<'_>) {
        out.headers_mut().insert("schema-version", self.0);
    }
}
// --8<-- [end:slot]

#[subscriber("orders", reply("receipts"))]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[derive(OutSlot)]
#[publishes(Audit)]
struct Trail;

#[subscriber("payments")]
async fn audit(payment: &Payment, Out(trail): Out<impl Publisher, Trail>) -> HandlerOutcome {
    let record = Audit { id: payment.id };
    if trail.message(&record).to("audit").publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // A transform follows the position it belongs to.
        b.include(confirm)
            .out_reply(Publish)
            .transform(Correlate)
            .include(audit)
            .out(Trail, Publish)
            .transform(SchemaVersion("2"))
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

    use super::{Audit, Order, Payment, Receipt, app};

    #[tokio::test]
    async fn a_receipt_carries_its_orders_correlation_id() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        let mut headers = HeaderMap::new();
        headers.insert("correlation-id", "c-17");
        broker
            .message(&Order { id: 17 })
            .to("orders")
            .with_headers(headers)
            .publish()
            .await?;

        broker
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { id: 17 })
            .with_header("correlation-id", "c-17");
        Ok(())
    }

    #[tokio::test]
    async fn an_audit_record_carries_the_schema_version() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Payment { id: 2 })
            .to("payments")
            .publish()
            .await?;

        broker
            .published::<Audit>("audit")
            .assert_called_once()
            .with(&Audit { id: 2 })
            .with_header("schema-version", "2");
        Ok(())
    }
}
