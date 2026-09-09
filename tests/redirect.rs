//! Redirected publishing: the `.redirect(..)` step names a destination per delivery, and an
//! ordinary transform still cannot.
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
    OutRedirect, Outgoing, PublishContext, PublishTransform, RedirectTransform,
};
use ruststream::testing::TestApp;

/// The one header the reply-to pattern reads, as a delivery carries it.
fn reply_to(name: &'static str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("reply-to", name);
    headers
}

/// The reply-to pattern the two motivating brokers implement: the answer goes where the request
/// asked to be answered, and to the declared fallback when it asked for nothing.
struct ReplyTo;

impl<C> RedirectTransform<C> for ReplyTo {
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>) {
        if let Some(to) = cx.headers().get("reply-to")
            && let Ok(to) = std::str::from_utf8(to)
        {
            out.set_name(to.to_owned());
        }
    }
}

/// An ordinary transform beside the redirect: it owns the headers and nothing else.
struct Stamp;

impl<C> PublishTransform<C> for Stamp {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {
        let destination = out.name().as_bytes().to_vec();
        out.headers_mut().insert("x-destination", destination);
    }
}

#[subscriber("redirect.requests", publish("redirect.receipts"))]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// The redirect names the reply's destination from the delivery it answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_redirect_sends_the_reply_where_the_request_asked() {
    let app = RustStream::new(AppInfo::new("redirect-reply", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm).out(Reply, Publish).redirect(ReplyTo);
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

/// A delivery the redirect leaves alone falls back to the destination the mount site declared -
/// the one the generated document reports.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_redirect_that_names_nothing_leaves_the_declared_destination() {
    let app = RustStream::new(AppInfo::new("redirect-fallback", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm).out(Reply, Publish).redirect(ReplyTo);
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

/// The redirect decides the destination first, whatever order the chain names the steps in: the
/// transform beside it reads the destination the redirect settled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_redirect_settles_the_destination_before_the_transforms_run() {
    let app = RustStream::new(AppInfo::new("redirect-order", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm)
                .out(Reply, Publish)
                .transform(Stamp)
                .redirect(ReplyTo);
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

/// Without a redirect the reply goes where the mount site says, and the transform stack cannot
/// move it: the destination it reads is the declared one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transform_alone_cannot_move_the_reply() {
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

impl OutRedirect for ByTenant {
    fn apply(&self, out: &mut Outgoing<'_>) {
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

/// A redirected slot publishes where its redirect says, and the slot's own capture records the
/// same message the broker received.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_redirected_slot_publishes_where_the_redirect_says() {
    let app = RustStream::new(AppInfo::new("redirect-slot", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(mirror)
                .out(Audit, Publish)
                .redirect(ByTenant)
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
    // The slot's own capture and the broker's log tell the same story: the redirect runs above
    // the attributed leaf, so the harness never reports a destination the broker did not see.
    let audited = tb.out::<Audit>().assert_called_once();
    assert_eq!(audited.messages()[0].name(), "redirect.audit.north");
}

/// The same slot without the step: the call site's own `.to(..)` stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_without_a_redirect_keeps_the_call_site_destination() {
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
/// through the definition, so the step is there and the redirect runs.
#[subscriber("redirect.mixed", publish("redirect.mixed.receipts"))]
async fn confirm_and_audit(order: &Order, Out(audit): Out<impl Publisher, Audit>) -> Receipt {
    let _ = audit.message(order).to("redirect.audit").publish().await;
    Receipt { id: order.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_redirect_rides_the_reply_of_a_slot_carrying_handler() {
    let app = RustStream::new(AppInfo::new("redirect-mixed", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm_and_audit)
                .out(Reply, Publish)
                .redirect(ReplyTo)
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
