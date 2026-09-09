//! Integration coverage for the non-default codecs through the real router include path.
//!
//! The codec unit tests in `src/codec/*` prove each codec round-trips in isolation; this drives a
//! `CborCodec` and a `MsgpackCodec` end to end - named on a router with `with_codec`, mounted on a
//! live app, fed a publish that names the same codec, and decoded back into a typed handler
//! argument - and drives the publish side of the same ladder, where one `Out` slot names its own
//! codec and its neighbour keeps the registration surface's.
#![cfg(all(
    feature = "macros",
    feature = "cbor",
    feature = "msgpack",
    feature = "memory",
    feature = "testing"
))]

mod common;

use std::future::Future;

use common::Order;
use ruststream::codec::{CborCodec, MsgpackCodec};
use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use ruststream::{HeaderMap, OutgoingMessage};

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

#[derive(OutSlot)]
#[publishes(Order)]
struct Ledger;

#[derive(OutSlot)]
#[publishes(Order)]
struct Audit;

/// Sends the same order through both slots, so a difference in what leaves them is the mount
/// site's doing and nothing else.
#[subscriber("orders-slots")]
async fn mirror(
    order: &Order,
    Out(ledger): Out<impl Publisher, Ledger>,
    Out(audit): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    if ledger
        .message(order)
        .to("orders-ledger")
        .publish()
        .await
        .is_err()
        || audit
            .message(order)
            .to("orders-audit")
            .publish()
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The innermost rung of the codec ladder on the publish side: `.out(marker, policy).codec(..)`
/// encodes what leaves that slot, while the slot naming none encodes with the registration
/// surface's codec - here the scope's `msgpack`, which is also what the request arrived in.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_codec_outranks_the_surface_codec_for_that_slot() {
    let app = RustStream::new(AppInfo::new("slot-codec", "0.1.0")).with_broker_codec(
        MemoryBroker::new(),
        MsgpackCodec,
        |b| {
            b.include(mirror)
                .out(Ledger, Publish)
                .codec(CborCodec)
                .out(Audit, Publish)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 7 })
        .with_codec(MsgpackCodec)
        .to("orders-slots")
        .publish()
        .await
        .expect("publish");

    tb.out::<Ledger>()
        .assert_called_once()
        .decoded_as::<Order>()
        .with_codec(&CborCodec, &Order { id: 7 });
    tb.out::<Audit>()
        .assert_called_once()
        .decoded_as::<Order>()
        .with_codec(&MsgpackCodec, &Order { id: 7 });
}

#[derive(OutSlot)]
#[publishes(Order)]
struct Keyed;

/// The shape a broker ships for a per-publish setting of its own: it wraps the publisher, adds
/// a header every message carries, and delegates the send.
struct Tagged<'a, P: ?Sized> {
    inner: &'a P,
    base: HeaderMap,
}

impl<P: Publisher + ?Sized> Publisher for Tagged<'_, P> {
    type Error = P::Error;

    fn publish(
        &self,
        msg: OutgoingMessage<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.inner.publish(msg)
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        Some(&self.base)
    }
}

/// Publishes through the adapter rather than straight through the slot.
#[subscriber("orders-adapter")]
async fn tagged_mirror(order: &Order, Out(out): Out<impl Publisher, Keyed>) -> HandlerOutcome {
    let mut base = HeaderMap::new();
    base.insert("lane", order.id.to_string());
    let tagged = Tagged { inner: out, base };

    if out
        .message_through(&tagged, order)
        .to("orders-tagged")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// An adapter in the way does not move the publish off the codec ladder: the bytes are the
/// slot's `cbor`, not the crate default, and the adapter's own header still reaches the wire.
/// Starting a fresh builder on the adapter is what used to resolve the default instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_through_an_adapter_keeps_the_mount_site_codec() {
    let app = RustStream::new(AppInfo::new("adapter-codec", "0.1.0")).with_broker_codec(
        MemoryBroker::new(),
        MsgpackCodec,
        |b| {
            b.include(tagged_mirror)
                .out(Keyed, Publish)
                .codec(CborCodec)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 7 })
        .with_codec(MsgpackCodec)
        .to("orders-adapter")
        .publish()
        .await
        .expect("publish");

    tb.out::<Keyed>()
        .assert_called_once()
        .decoded_as::<Order>()
        .with_codec(&CborCodec, &Order { id: 7 })
        .with_header("lane", "7");

    tb.broker::<MemoryBroker>()
        .published::<Order>("orders-tagged")
        .assert_called_once()
        .with_codec(&CborCodec, &Order { id: 7 });
}
