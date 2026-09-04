//! Integration coverage for the non-default codecs through the real router include path.
//!
//! The codec unit tests in `src/codec/*` prove each codec round-trips in isolation; this drives a
//! `CborCodec` and a `MsgpackCodec` end to end - named on a router with `with_codec`, mounted on a
//! live app, fed a publish that names the same codec, and decoded back into a typed handler
//! argument.
#![cfg(all(
    feature = "macros",
    feature = "cbor",
    feature = "msgpack",
    feature = "memory",
    feature = "testing"
))]

mod common;

use common::Order;
use ruststream::codec::{CborCodec, MsgpackCodec};
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, Router, RustStream};
use ruststream::subscriber;
use ruststream::testing::TestApp;

#[subscriber("orders-cbor")]
async fn cbor_order(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber("orders-msgpack")]
async fn msgpack_order(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// A `cbor` router and a `msgpack` router share one app: each decodes payloads its own codec
/// encoded, proving the router-scope codec selection works for the non-default codecs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_default_codecs_dispatch_through_the_router() {
    let cbor_router = Router::<MemoryBroker>::new()
        .with_codec(CborCodec)
        .include(cbor_order);
    let msgpack_router = Router::<MemoryBroker>::new()
        .with_codec(MsgpackCodec)
        .include(msgpack_order);

    let app =
        RustStream::new(AppInfo::new("codecs", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include_router(cbor_router);
            b.include_router(msgpack_router);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    // Each publish names the router's own codec, so the bytes on the wire are the ones that
    // router decodes with.
    tb.message(&Order { id: 7 })
        .with_codec(CborCodec)
        .to("orders-cbor")
        .publish()
        .await
        .expect("publish");
    tb.message(&Order { id: 7 })
        .with_codec(MsgpackCodec)
        .to("orders-msgpack")
        .publish()
        .await
        .expect("publish");

    // Each handler saw the order its own codec decoded: a mismatch would settle as a decode
    // failure instead.
    tb.broker::<MemoryBroker>()
        .subscriber("orders-cbor")
        .assert_called_once()
        .with_codec(&CborCodec, &Order { id: 7 })
        .settled(HandlerOutcome::ack());
    tb.broker::<MemoryBroker>()
        .subscriber("orders-msgpack")
        .assert_called_once()
        .with_codec(&MsgpackCodec, &Order { id: 7 })
        .settled(HandlerOutcome::ack());
}
