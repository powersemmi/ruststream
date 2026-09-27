//! The lanes that carry their own bytes need no codec, so they must compile in a build with no
//! codec feature at all: a generated message that serializes and deserializes itself rides both
//! lanes and the typed publish entry point over them.
//!
//! This file is the negative half of the codec surface. It is deliberately compiled only when no
//! codec feature is on, which is what `cargo check --no-default-features --features
//! testing,memory,macros --all-targets` (the codec-free gate) exercises.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "testing",
    not(any(feature = "json", feature = "cbor", feature = "msgpack"))
))]

use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::testing::TestApp;

/// A generated Protobuf message, decorated exactly as a `prost_build` config decorates what it
/// emits: our two lane derives and one `#[wire(prost)]` line. The type serializes itself, so no
/// codec is resolved for it - which is why it compiles in this file at all.
#[derive(Clone, PartialEq, prost::Message, Outgoing, Serialized, Deserialized)]
#[outgoing(name = "codecfree.orders")]
#[wire(prost)]
struct Order {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(string, tag = "2")]
    sku: String,
}

// The settlement carries what the generated decoder produced, so the assertion below sees
// whether the fields survived the round trip - not only that some bytes arrived.
#[subscriber("codecfree.orders")]
async fn take_order(order: &Order) -> HandlerOutcome {
    if order.id == 7 && order.sku == "widget" {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_generated_message_rides_both_lanes_with_no_codec() {
    let app =
        RustStream::new(AppInfo::new("codecfree", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(take_order);
        });
    let tb = TestApp::start(app).await.expect("harness start");

    let order = Order {
        id: 7,
        sku: "widget".to_owned(),
    };
    // The message names no codec on the way out and none on the way in; the type owns both ends.
    tb.message(&order).publish().await.expect("inject");

    tb.broker::<MemoryBroker>()
        .subscriber("codecfree.orders")
        .assert_called_once()
        .with_raw(&prost::Message::encode_to_vec(&order))
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("graceful shutdown");
}
