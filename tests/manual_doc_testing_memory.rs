//! The macro-free counterpart of `tests/doc_testing_memory.rs`: the same reply handler and the
//! same `TestApp` test, with the definition written out instead of declared by `#[subscriber]`.
//!
//! What the harness reads is the definition, not the attribute, so the test half is unchanged.
#![cfg(all(feature = "testing", feature = "memory", feature = "json"))]

// --8<-- [start:handler]
use std::future::{Future, ready};

use ruststream::prelude::*;
use ruststream::runtime::{
    AllOpen, Declared, Decoded, OutgoingMessageMetadata, PublishingCall, PublishingDef,
    SubscriberBuilder, forms,
};
use ruststream::{CallerName, MessageHeaders, NoHeaders, OutgoingDestination};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct Order {
    id: u64,
    quantity: u32,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the destination stays with the call site, which
// is what lets the test publish this type with `.to("orders")`.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct Confirmation {
    id: u64,
    accepted: bool,
}

/// The subscription source, the reply destination and the reply type live on `PublishingDef`;
/// the body moves to `PublishingCall`, which stays generic over the state so it mounts on an app
/// with any state type.
struct Confirm;

impl Declared for Confirm {
    type Form = forms::Publishing;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("orders"))
    }
}

impl PublishingDef for Confirm {
    type Input = Decoded<Order>;
    type Injections = ();
    type Reply = Confirmation;
    type Context = ();
    type Source = Name;

    fn source(&self) -> Self::Source {
        Name::new("orders")
    }

    fn reply_name(&self) -> &'static str {
        "confirmations"
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        vec![OutgoingMessageMetadata::new(
            "confirmations",
            std::any::type_name::<Confirmation>(),
        )]
    }
}

impl<State: Send + Sync> PublishingCall<State> for Confirm {
    fn call(
        &self,
        order: &Order,
        _injections: &Self::Injections,
        _ctx: &mut Context<'_, (), State>,
    ) -> impl Future<Output = Result<Confirmation, HandlerResult>> + Send {
        ready(Ok(Confirmation {
            id: order.id,
            accepted: order.quantity > 0,
        }))
    }
}
// --8<-- [end:handler]

// --8<-- [start:test]
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::testing::TestApp;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirms_valid_orders() {
    // The app under test: production wiring, in-memory broker.
    let app = RustStream::new(AppInfo::new("orders-test", "0.0.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            let replies = TypedPublisher::new(MemoryPublish);
            b.include(Confirm).publisher(replies);
        },
    );

    // Start the harness (no connect, no server) and publish an order; the publish drives the
    // handler to a standstill before it returns.
    let tb = TestApp::start(app).await.expect("start harness");
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 1, quantity: 2 })
        .to("orders")
        .publish()
        .await
        .expect("publish");

    // The handler decoded the order, acked, and published a confirmation.
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1, quantity: 2 })
        .settled(HandlerResult::Ack);
    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("confirmations")
        .assert_called_once()
        .with(&Confirmation {
            id: 1,
            accepted: true,
        });
}
// --8<-- [end:test]
