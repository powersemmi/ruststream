//! Integration coverage for the non-default codecs through the real router include path.
//!
//! The codec unit tests in `src/codec/*` prove each codec round-trips in isolation; this drives a
//! `CborCodec` and a `MsgpackCodec` end to end - named on a router with `with_codec`, mounted on a
//! live app, fed a publish that names the same codec, and decoded back into a typed handler
//! argument.
#![cfg(all(feature = "macros", feature = "cbor", feature = "msgpack"))]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{Order, wait_for};
use ruststream::codec::{CborCodec, MsgpackCodec};
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, PublishExt, Router, RustStream};
use ruststream::subscriber;

static CBOR_SEEN: AtomicUsize = AtomicUsize::new(0);
static MSGPACK_SEEN: AtomicUsize = AtomicUsize::new(0);

#[subscriber("orders-cbor")]
async fn cbor_order(order: &Order) -> HandlerOutcome {
    assert_eq!(order.id, 7);
    CBOR_SEEN.fetch_add(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

#[subscriber("orders-msgpack")]
async fn msgpack_order(order: &Order) -> HandlerOutcome {
    assert_eq!(order.id, 7);
    MSGPACK_SEEN.fetch_add(1, Ordering::SeqCst);
    HandlerOutcome::ack()
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

    // Each publish names the router's own codec, so the bytes on the wire are the ones that
    // router decodes with.
    publisher
        .message(&Order { id: 7 })
        .with_codec(CborCodec)
        .to("orders-cbor")
        .publish()
        .await
        .expect("publish");
    publisher
        .message(&Order { id: 7 })
        .with_codec(MsgpackCodec)
        .to("orders-msgpack")
        .publish()
        .await
        .expect("publish");

    wait_for(
        || CBOR_SEEN.load(Ordering::SeqCst) >= 1 && MSGPACK_SEEN.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(5),
    )
    .await;

    running.shutdown().await.expect("graceful shutdown failed");
}
