//! Per-message settings: a broker's priority, `QoS` or key, defaulted by the publish policy at the
//! mount site and set for one publish by a step the broker adds to the builder. The scenarios are
//! tests.
//!
//! ```text
//! cargo test --example per_message_options --features testing,macros,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryPublisher};
use ruststream::runtime::{Outgoing, PublishBuilder, PublishSink};
use ruststream::{OutgoingMessage, PairError, Take};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    express: bool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

// The broker side, as a broker crate ships it: per-message settings, a policy holding their
// defaults, and a builder step. The memory broker's own publisher takes no settings (its
// `Options` is `()`), so this one wraps it and turns the priority into a header.

/// The broker's per-message settings: every field optional, unset means "the policy's default".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PriorityOptions {
    priority: Option<u8>,
}

/// The publish policy: what the mount site names, carrying the default priority.
#[derive(Debug, Clone, Copy, Default)]
struct PriorityPublish {
    priority: u8,
}

impl PriorityPublish {
    const fn priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }
}

/// The live publisher the policy pairs with the connected broker.
struct PriorityPublisher {
    inner: MemoryPublisher,
    default_priority: u8,
}

impl PublishPolicy<ConnectedMemoryBroker> for PriorityPublish {
    type Live = PriorityPublisher;

    async fn pair(self, connected: &ConnectedMemoryBroker) -> Result<Self::Live, PairError> {
        Ok(PriorityPublisher {
            inner: Publish.pair(connected).await?,
            default_priority: self.priority,
        })
    }
}

impl Publisher for PriorityPublisher {
    type Payload = Take;
    type Error = MemoryError;
    type Options = PriorityOptions;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let priority = options
            .and_then(|options| options.priority)
            .unwrap_or(self.default_priority);
        let mut headers = msg.headers().clone();
        headers.insert("priority", priority.to_string());
        let stamped = OutgoingMessage::new(msg.name(), msg.payload()).with_headers(headers);
        self.inner.publish(stamped, None).await
    }
}

/// The step the broker puts in its prelude, offered only on a builder over its own publisher.
trait PriorityPublishSteps {
    /// Sends this one message at `priority`, whatever the mount site's default is.
    #[must_use]
    fn priority(self, priority: u8) -> Self;
}

impl<Sink, Body, Enc, Hdrs, Dest> PriorityPublishSteps
    for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
where
    Sink: PublishSink<Options = PriorityOptions>,
{
    fn priority(mut self, priority: u8) -> Self {
        self.options_mut()
            .get_or_insert_with(PriorityOptions::default)
            .priority = Some(priority);
        self
    }
}

#[derive(OutSlot)]
#[publishes(Order)]
struct Shipments;

// --8<-- [start:handler]
/// The bound names the broker's settings, so the broker's own steps appear on the builder.
#[subscriber("orders")]
async fn ship(
    order: &Order,
    Out(out): Out<impl Publisher<Options = PriorityOptions>, Shipments>,
) -> HandlerOutcome {
    let shipment = out.message(order).to("shipments");
    let sent = if order.express {
        shipment.priority(9).publish().await
    } else {
        shipment.publish().await
    };
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:transform]
/// A reply has no call site, so a transform sets its settings.
struct LowPriority;

impl<K: ContextKind> PublishTransform<K, PriorityOptions> for LowPriority {
    type Destination = Reads;

    fn apply(
        &self,
        _out: &mut Outgoing<'_>,
        options: &mut Option<PriorityOptions>,
        _cx: &K::View<'_>,
    ) {
        options
            .get_or_insert_with(PriorityOptions::default)
            .priority = Some(1);
    }
}

#[subscriber("receipt-requests", reply)]
async fn issue_receipt(order: &Order) -> Receipt {
    Receipt { order_id: order.id }
}
// --8<-- [end:transform]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // The policy holds the defaults: priority 3 unless a step says otherwise.
        b.include(ship)
            .out(Shipments, PriorityPublish::default().priority(3))
            .include(issue_receipt)
            .out_reply(PriorityPublish::default().priority(3))
            .transform(LowPriority)
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

    use super::{Order, PriorityOptions, Receipt, app};

    #[tokio::test]
    async fn an_express_order_ships_at_top_priority() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order {
                id: 7,
                express: true,
            })
            .to("orders")
            .publish()
            .await?;

        broker
            .published::<Order>("shipments")
            .assert_called_once()
            .with_header("priority", "9")
            .with_options(&PriorityOptions { priority: Some(9) });
        Ok(())
    }

    #[tokio::test]
    async fn a_standard_order_ships_at_the_policy_default() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order {
                id: 8,
                express: false,
            })
            .to("orders")
            .publish()
            .await?;

        broker
            .published::<Order>("shipments")
            .assert_called_once()
            .with_header("priority", "3")
            .assert_options_default();
        Ok(())
    }

    #[tokio::test]
    async fn a_receipt_leaves_at_the_priority_its_transform_sets() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order {
                id: 4,
                express: false,
            })
            .to("receipt-requests")
            .publish()
            .await?;

        broker
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 4 })
            .with_header("priority", "1");
        Ok(())
    }
}
