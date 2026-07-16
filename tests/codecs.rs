//! Integration coverage for the non-default codecs through the real router include path.
//!
//! The codec unit tests in `src/codec/*` prove each codec round-trips in isolation; this drives a
//! `CborCodec` and a `MsgpackCodec` end to end - named on a router with `with_codec`, mounted on a
//! live app, fed wire bytes that codec produced, and decoded back into a typed handler argument.
//! Without this, only the default `json` codec ever travelled the dispatch path.
#![cfg(all(feature = "macros", feature = "cbor", feature = "msgpack"))]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::wait_for;
use ruststream::codec::{CborCodec, Codec, MsgpackCodec};
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, Router, RustStream};
use ruststream::{OutgoingMessage, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

static CBOR_SEEN: AtomicUsize = AtomicUsize::new(0);
static MSGPACK_SEEN: AtomicUsize = AtomicUsize::new(0);

#[subscriber("orders-cbor")]
async fn cbor_order(order: &Order) -> HandlerResult {
    assert_eq!(order.id, 7);
    CBOR_SEEN.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

#[subscriber("orders-msgpack")]
async fn msgpack_order(order: &Order) -> HandlerResult {
    assert_eq!(order.id, 7);
    MSGPACK_SEEN.fetch_add(1, Ordering::SeqCst);
    HandlerResult::Ack
}

/// A `cbor` router and a `msgpack` router share one app: each decodes payloads its own codec
/// encoded, proving the router-scope codec selection works for the non-default codecs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_default_codecs_dispatch_through_the_router() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let cbor_router = Router::<MemoryBroker>::new()
        .with_codec(CborCodec)
        .include(cbor_order);
    let msgpack_router = Router::<MemoryBroker>::new()
        .with_codec(MsgpackCodec)
        .include(msgpack_order);

    let app = RustStream::new(AppInfo::new("codecs", "0.1.0")).with_broker(broker, |b| {
        b.include_router(cbor_router);
        b.include_router(msgpack_router);
    });

    // `start` resolves only once subscriptions are open, so one publish per codec suffices.
    let running = app.start().await.expect("startup failed");

    let cbor_bytes = CborCodec.encode(&Order { id: 7 }).unwrap();
    let msgpack_bytes = MsgpackCodec.encode(&Order { id: 7 }).unwrap();
    publisher
        .publish(OutgoingMessage::new("orders-cbor", &cbor_bytes))
        .await
        .expect("publish");
    publisher
        .publish(OutgoingMessage::new("orders-msgpack", &msgpack_bytes))
        .await
        .expect("publish");

    wait_for(
        || CBOR_SEEN.load(Ordering::SeqCst) >= 1 && MSGPACK_SEEN.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(5),
    )
    .await;

    running.shutdown().await.expect("graceful shutdown failed");
}
