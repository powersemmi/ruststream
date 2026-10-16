//! The assertions over the per-message settings a publish carried: the value a builder step set,
//! or none, so the publish policy's defaults applied. Every scenario is a test of its own, the
//! way a service writes it.
//!
//! ```text
//! cargo test --example testing_published_options --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryError, MemoryPublisher};
use ruststream::runtime::{PublishBuilder, PublishSink};
use ruststream::{BytesMut, OutgoingMessage, PairError, Take};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    express: bool,
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

mod with_options {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, PriorityOptions, PriorityPublish, PriorityPublishSteps};

    // --8<-- [start:with-options]
    #[derive(OutSlot)]
    #[publishes(Order)]
    struct Shipments;

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

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(ship)
                .out(Shipments, PriorityPublish::default().priority(3))
                .build();
        })
    }

    #[tokio::test]
    async fn an_express_order_ships_at_top_priority() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MemoryBroker>()
            .message(&Order {
                id: 7,
                express: true,
            })
            .to("orders")
            .publish()
            .await?;

        tb.out::<Shipments>()
            .assert_called_once()
            .with_options(&PriorityOptions { priority: Some(9) });
        Ok(())
    }
    // --8<-- [end:with-options]
}

mod assert_options_default {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, PriorityOptions, PriorityPublish, PriorityPublishSteps};

    // --8<-- [start:assert-options-default]
    #[derive(OutSlot)]
    #[publishes(Order)]
    struct Shipments;

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

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(ship)
                .out(Shipments, PriorityPublish::default().priority(3))
                .build();
        })
    }

    #[tokio::test]
    async fn a_standard_order_ships_at_the_default_priority() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MemoryBroker>()
            .message(&Order {
                id: 7,
                express: false,
            })
            .to("orders")
            .publish()
            .await?;

        tb.out::<Shipments>()
            .assert_called_once()
            .assert_options_default();
        Ok(())
    }
    // --8<-- [end:assert-options-default]
}

fn main() {
    println!("the scenarios are tests: cargo test --example testing_published_options");
}
