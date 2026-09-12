//! Redirected publishing: the `.transform(..)` step hands a transform the destination, and the
//! declaration stands wherever the step is not named.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

mod common;

use common::{Order, Receipt};

use ruststream::memory::prelude::*;
use ruststream::runtime::{
    ContextKind, ForReply, Names, Outgoing, PublishContext, PublishTransform, Reads,
};
use ruststream::testing::TestApp;

/// The one header the reply-to pattern reads, as a delivery carries it.
fn reply_to(name: &'static str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("reply-to", name);
    headers
}

/// The reply-to pattern the two motivating brokers implement: the answer goes where the request
/// asked to be answered, and to the declared fallback when it asked for nothing. It declares
/// `Names`, so it mounts only where the position offers the right to set the destination.
struct ReplyTo;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ReplyTo {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(to) = cx.headers().get("reply-to")
            && let Ok(to) = std::str::from_utf8(to)
        {
            out.set_name(to.to_owned());
        }
    }
}

/// An ordinary transform beside the redirect: it owns the headers and nothing else.
struct Stamp;

impl<K: ContextKind, Options> PublishTransform<K, Options> for Stamp {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &K::View<'_>) {
        let destination = out.name().as_bytes().to_vec();
        out.headers_mut().insert("x-destination", destination);
    }
}

#[subscriber("redirect.requests", publish("redirect.receipts"))]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// The transform names the reply's destination from the delivery it answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_naming_transform_sends_the_reply_where_the_request_asked() {
    let app = RustStream::new(AppInfo::new("redirect-reply", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm).out(Reply, Publish).transform(ReplyTo);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 4 })
        .with_headers(reply_to("redirect.inbox.4"))
        .to("redirect.requests")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("redirect.inbox.4")
        .assert_called_once()
        .with(&Receipt { id: 4 });
}

/// A delivery the transform leaves alone falls back to the destination the mount site declared -
/// the one the generated document reports.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_naming_transform_that_sets_nothing_leaves_the_declared_destination() {
    let app = RustStream::new(AppInfo::new("redirect-fallback", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm).out(Reply, Publish).transform(ReplyTo);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 5 })
        .to("redirect.requests")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("redirect.receipts")
        .assert_called_once()
        .with(&Receipt { id: 5 });
}

/// The chain runs in the order the caller wrote it: a transform after the naming one reads the
/// destination that one settled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_chain_runs_in_the_order_the_caller_wrote() {
    let app = RustStream::new(AppInfo::new("redirect-order", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm)
                .out(Reply, Publish)
                .transform(ReplyTo)
                .transform(Stamp);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 6 })
        .with_headers(reply_to("redirect.inbox.6"))
        .to("redirect.requests")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("redirect.inbox.6")
        .assert_called_once()
        .with_header("x-destination", b"redirect.inbox.6");
}

/// Without the step, the reply goes where the mount site declared, and the transform stack runs
/// over that destination rather than deciding it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_one_the_declared_destination_stands() {
    let app = RustStream::new(AppInfo::new("redirect-none", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm).out(Reply, Publish).transform(Stamp);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 7 })
        .with_headers(reply_to("redirect.inbox.7"))
        .to("redirect.requests")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("redirect.receipts")
        .assert_called_once()
        .with_header("x-destination", b"redirect.receipts");
}

/// The slot counterpart: a shard router deciding where each message goes.
struct ByTenant;

impl<K: ContextKind, Options> PublishTransform<K, Options> for ByTenant {
    type Destination = Names;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &K::View<'_>) {
        if let Some(tenant) = out.headers().get("x-tenant")
            && let Ok(tenant) = std::str::from_utf8(tenant)
        {
            out.set_name(format!("redirect.audit.{tenant}"));
        }
    }
}

#[derive(OutSlot)]
#[publishes(Order)]
struct Audit;

#[subscriber("redirect.orders")]
async fn mirror(order: &Order, Out(audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    headers.insert("x-tenant", "north");
    let sent = audit
        .message(order)
        .with_headers(headers)
        .to("redirect.audit")
        .publish()
        .await;
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// A slot whose transform names destinations publishes where it says, and the slot's own capture
/// records the same message the broker received.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_publishes_where_its_naming_transform_says() {
    let app = RustStream::new(AppInfo::new("redirect-slot", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(mirror)
                .out(Audit, Publish)
                .transform(ByTenant)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 9 })
        .to("redirect.orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Order>("redirect.audit.north")
        .assert_called_once()
        .with(&Order { id: 9 });
    // The slot's own capture and the broker's log tell the same story: the transform runs above
    // the attributed leaf, so the harness never reports a destination the broker did not see.
    let audited = tb.out::<Audit>().assert_called_once();
    assert_eq!(audited.messages()[0].name(), "redirect.audit.north");
}

/// The same slot without such a transform: the call site's own `.to(..)` stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_without_one_keeps_the_call_site_destination() {
    let app = RustStream::new(AppInfo::new("redirect-slot-none", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(mirror).out(Audit, Publish).build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 10 })
        .to("redirect.orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Order>("redirect.audit")
        .assert_called_once()
        .with(&Order { id: 10 });
}

/// The reply position of a handler that also carries a slot: the chain reads the reply type
/// through the definition, so the offer is known and the naming transform mounts.
#[subscriber("redirect.mixed", publish("redirect.mixed.receipts"))]
async fn confirm_and_audit(order: &Order, Out(audit): Out<impl Publisher, Audit>) -> Receipt {
    let _ = audit.message(order).to("redirect.audit").publish().await;
    Receipt { id: order.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_naming_transform_rides_the_reply_of_a_slot_carrying_handler() {
    let app = RustStream::new(AppInfo::new("redirect-mixed", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm_and_audit)
                .out(Reply, Publish)
                .transform(ReplyTo)
                .out(Audit, Publish)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 12 })
        .with_headers(reply_to("redirect.inbox.12"))
        .to("redirect.mixed")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("redirect.inbox.12")
        .assert_called_once()
        .with(&Receipt { id: 12 });
}

/// A marker with no dictionary offers no naming right, and that withholds the right rather than
/// transforms as such: the implicit `DefaultSlot` still takes an ordinary transform.
#[subscriber("redirect.plain")]
async fn plain(order: &Order, Out(out): Out<impl Publisher, DefaultSlot>) -> HandlerOutcome {
    if out
        .message(order)
        .to("redirect.plain.out")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_marker_without_a_dictionary_still_takes_an_ordinary_transform() {
    let app = RustStream::new(AppInfo::new("redirect-default-slot", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(plain)
                .out(DefaultSlot, Publish)
                .transform(Stamp)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 13 })
        .to("redirect.plain")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Order>("redirect.plain.out")
        .assert_called_once()
        .with_header("x-destination", b"redirect.plain.out");
}
