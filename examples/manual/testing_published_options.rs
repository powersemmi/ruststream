//! The assertions over the per-message settings a publish carried, against services written
//! without the `macros` feature. The test bodies are the ones of
//! `examples/testing_published_options.rs`: the harness reads the same records whichever way the
//! service was declared.
//!
//! ```text
//! cargo test --example manual_testing_published_options --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryError, MemoryPublisher};
use ruststream::runtime::{PublishBuilder, PublishSink};
use ruststream::{BytesMut, OutgoingMessage, PairError, Take};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
    express: bool,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

impl<M: OutSlot> OutMessages<M> for Order {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        Vec::new()
    }
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
    use ruststream::runtime::{PublishedThrough, Reads};
    use ruststream::testing::TestApp;

    use super::{Order, PriorityOptions, PriorityPublish, PriorityPublishSteps};

    struct Shipments;

    impl OutSlot for Shipments {
        const NAME: &'static str = "Shipments";
        type Destination = Reads;

        fn outgoing() -> Vec<OutgoingMessageMetadata> {
            <Order as OutMessages<Self>>::outgoing()
        }
    }

    impl PublishedThrough<Shipments> for Order {}

    struct Ship;

    impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Ship
    where
        Entry: OutEntry<Shipments, Wire: Publisher<Options = PriorityOptions>>,
    {
        async fn handle(
            &self,
            order: &Order,
            outs: &Outs<(Entry,)>,
            _ctx: &mut Context<'_>,
        ) -> Result<(), HandlerOutcome> {
            let shipment = outs.get(Shipments).message(order).to("shipments");
            let sent = if order.express {
                shipment.priority(9).publish().await
            } else {
                shipment.publish().await
            };
            if sent.is_err() {
                return Err(HandlerOutcome::retry());
            }
            Ok(())
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Ship).build())
                .out(Shipments, PriorityPublish::default().priority(3))
                .build();
        })
    }

    // --8<-- [start:with-options]
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
    use ruststream::runtime::{PublishedThrough, Reads};
    use ruststream::testing::TestApp;

    use super::{Order, PriorityOptions, PriorityPublish, PriorityPublishSteps};

    struct Shipments;

    impl OutSlot for Shipments {
        const NAME: &'static str = "Shipments";
        type Destination = Reads;

        fn outgoing() -> Vec<OutgoingMessageMetadata> {
            <Order as OutMessages<Self>>::outgoing()
        }
    }

    impl PublishedThrough<Shipments> for Order {}

    struct Ship;

    impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Ship
    where
        Entry: OutEntry<Shipments, Wire: Publisher<Options = PriorityOptions>>,
    {
        async fn handle(
            &self,
            order: &Order,
            outs: &Outs<(Entry,)>,
            _ctx: &mut Context<'_>,
        ) -> Result<(), HandlerOutcome> {
            let shipment = outs.get(Shipments).message(order).to("shipments");
            let sent = if order.express {
                shipment.priority(9).publish().await
            } else {
                shipment.publish().await
            };
            if sent.is_err() {
                return Err(HandlerOutcome::retry());
            }
            Ok(())
        }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Ship).build())
                .out(Shipments, PriorityPublish::default().priority(3))
                .build();
        })
    }

    // --8<-- [start:assert-options-default]
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

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
