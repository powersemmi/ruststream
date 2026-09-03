//! The publish path of an `Out` slot: the app-wide `publish_layer` chain over every slot publish,
//! and the per-slot `.out(marker, policy).transform(..)` step over one slot's own.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::error::Error;

use common::Order;

use ruststream::memory::prelude::*;
use ruststream::runtime::{
    OutTransform, Outgoing, PublishContext, PublishLayer, PublishNext, PublishPipeline,
    PublishTransform,
};
use ruststream::testing::TestApp;

/// The app-wide middleware: it stamps every publish the service makes, replies and slots alike.
#[derive(Clone)]
struct AppStamp;

impl PublishLayer for AppStamp {
    async fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        out.headers_mut().insert("x-app", b"1".to_vec());
        next.run(out).await
    }
}

/// A per-slot transform: the outbox envelope one destination wants and the others do not.
struct Envelope;

impl OutTransform for Envelope {
    fn apply(&self, out: &mut Outgoing<'_>) {
        out.headers_mut().insert("x-outbox", b"1".to_vec());
    }
}

/// A per-reply transform, stamping the delivery the reply answers.
struct StampSource;

impl<C> PublishTransform<C> for StampSource {
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>) {
        out.headers_mut()
            .insert("x-source", cx.name().as_bytes().to_vec());
    }
}

#[derive(OutSlot)]
#[publishes(Order)]
struct Audit;

#[derive(OutSlot)]
#[publishes(Order)]
struct Journal;

/// Publishes the same order through both slots, so a stamp that lands on one and not the other
/// is the mount site's doing.
#[subscriber("out.orders")]
async fn mirror(
    order: &Order,
    Out(audit): Out<impl Publisher, Audit>,
    Out(journal): Out<impl Publisher, Journal>,
) -> HandlerOutcome {
    if audit
        .message(order)
        .to("out.audit")
        .publish()
        .await
        .is_err()
        || journal
            .message(order)
            .to("out.journal")
            .publish()
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The app-wide publish middleware wraps a slot publish, and it runs above the slot's attributed
/// leaf: the per-slot capture and the broker's log see the same stamped message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_app_wide_publish_layer_stamps_every_slot_publish() {
    let app = RustStream::new(AppInfo::new("out-layer", "0.1.0"))
        .publish_layer(AppStamp)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(mirror)
                .out(Audit, Publish)
                .out(Journal, Publish)
                .build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 7 })
        .to("out.orders")
        .publish()
        .await
        .expect("publish");

    tb.out::<Audit>()
        .assert_called_once()
        .with_header("x-app", b"1");
    tb.out::<Journal>()
        .assert_called_once()
        .with_header("x-app", b"1");
    tb.broker::<MemoryBroker>()
        .published::<Order>("out.audit")
        .assert_called_once()
        .with(&Order { id: 7 })
        .with_header("x-app", b"1");
}

/// `.transform(..)` rides the slot the `.out(..)` before it bound, and only that one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_transform_rides_the_slot_it_follows() {
    let app = RustStream::new(AppInfo::new("out-transform", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(mirror)
                .out(Audit, Publish)
                .transform(Envelope)
                .out(Journal, Publish)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 3 })
        .to("out.orders")
        .publish()
        .await
        .expect("publish");

    tb.out::<Audit>()
        .assert_called_once()
        .with_header("x-outbox", b"1");
    let journal = tb.out::<Journal>().assert_called_once();
    assert_eq!(
        journal.messages()[0].headers().get("x-outbox"),
        None,
        "the transform belongs to the slot it was named on",
    );
}

/// The order of the two calls does not matter: each transform lands on its own slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_slot_keeps_its_own_transform_stack() {
    let app = RustStream::new(AppInfo::new("out-transform-both", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(mirror)
                .out(Journal, Publish)
                .out(Audit, Publish)
                .transform(Envelope)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 11 })
        .to("out.orders")
        .publish()
        .await
        .expect("publish");

    tb.out::<Audit>()
        .assert_called_once()
        .with_header("x-outbox", b"1");
    let journal = tb.out::<Journal>().assert_called_once();
    assert_eq!(journal.messages()[0].headers().get("x-outbox"), None);
}

/// A handler that both replies and publishes through a slot: the reply's transform and the
/// slot's stay on their own message, while the app-wide layer stamps both.
#[subscriber("out.requests", publish("out.receipts"))]
async fn confirm(order: &Order, Out(audit): Out<impl Publisher, Audit>) -> Order {
    let _ = audit.message(order).to("out.audit").publish().await;
    Order { id: order.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_and_a_slot_carry_their_own_transforms_under_one_app_layer() {
    let app = RustStream::new(AppInfo::new("out-reply-and-slot", "0.1.0"))
        .publish_layer(AppStamp)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(confirm)
                .publisher(Publish)
                .transform(StampSource)
                .out(Audit, Publish)
                .transform(Envelope)
                .build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 5 })
        .to("out.requests")
        .publish()
        .await
        .expect("publish");

    let reply = tb
        .broker::<MemoryBroker>()
        .published::<Order>("out.receipts")
        .assert_called_once()
        .with_header("x-app", b"1")
        .with_header("x-source", b"out.requests");
    assert_eq!(
        reply.messages()[0].headers().get("x-outbox"),
        None,
        "the slot's transform does not reach the reply",
    );

    let audit = tb
        .out::<Audit>()
        .assert_called_once()
        .with_header("x-app", b"1")
        .with_header("x-outbox", b"1");
    assert_eq!(
        audit.messages()[0].headers().get("x-source"),
        None,
        "the reply's transform does not reach the slot",
    );
}
