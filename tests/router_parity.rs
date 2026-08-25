//! Router parity: every definition form the broker scope mounts is reachable from a standalone
//! [`Router`], through the same form-token dispatch.
//!
//! The scope's own coverage of these forms lives next to each feature (`out_injection`,
//! `out_slots`, `seek_api`, ...); what is asserted here is that the router surface reaches them
//! too, with the terminals a consuming builder needs.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

mod common;

use common::{Event, connected, expect_id, observed_memory, payload};

use tokio::sync::Notify;

use ruststream::memory::{MemoryBroker, MemoryPosition, MemoryPublish, MemorySeeker, MemorySource};
use ruststream::runtime::{AppInfo, HandlerResult, Out, PublishExt, Router, RustStream, Seek};
use ruststream::testing::TestApp;
use ruststream::{Broker, OutSlot, Publisher, Seeker, subscriber};

// ---------------------------------------------------------------------------------------------
// Out slots: the single-slot shorthand, named slots, and the batch counterpart.

#[subscriber("rp.out.in")]
async fn forward(event: &Event, Out(out): Out<impl Publisher>) -> HandlerResult {
    if out
        .raw(&payload(event.id))
        .to("rp.out.forwarded")
        .publish()
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

/// The one-slot shorthand on a router: `.publisher(policy)` binds the slot and commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_mounts_a_single_out_slot() {
    let (broker, ingress, observer) = observed_memory().await;

    let router = Router::<MemoryBroker>::new()
        .include(forward)
        .publisher(MemoryPublish);
    let app = RustStream::new(AppInfo::new("rp-out", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));
    let running = app.start().await.expect("startup failed");

    ingress
        .raw(&payload(3))
        .to("rp.out.in")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "rp.out.forwarded", 3).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

#[derive(OutSlot)]
struct Encoded;

#[derive(OutSlot)]
struct Audit;

#[subscriber("rp.slots.in", raw)]
async fn transcode(
    chunk: &[u8],
    Out(encoded): Out<impl Publisher, Encoded>,
    Out(audit): Out<impl Publisher, Audit>,
) -> HandlerResult {
    if encoded
        .raw(chunk)
        .to("rp.slots.encoded")
        .publish()
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    let receipt = chunk.len().to_be_bytes();
    if audit
        .raw(&receipt)
        .to("rp.slots.audit")
        .publish()
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

/// Named slots bind by marker in any order, and `.mount()` is the terminal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_binds_named_out_slots_by_marker() {
    // Deliberately bound in the opposite of the signature order.
    let router = Router::<MemoryBroker>::new()
        .include(transcode)
        .out(Audit, MemoryPublish)
        .out(Encoded, MemoryPublish)
        .mount();
    let app = RustStream::new(AppInfo::new("rp-slots", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .raw(b"frame")
        .to("rp.slots.in")
        .publish()
        .await
        .expect("raw publish");

    tb.out::<Encoded>().assert_called_once().with_raw(b"frame");
    tb.out::<Audit>().assert_called_once();
}

#[subscriber(batch("rp.page.in"))]
async fn forward_page(events: &[Event], Out(out): Out<impl Publisher>) -> HandlerResult {
    for event in events {
        if out
            .raw(&payload(event.id))
            .to("rp.page.forwarded")
            .publish()
            .await
            .is_err()
        {
            return HandlerResult::retry();
        }
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_mounts_a_batch_out_slot() {
    let (broker, ingress, observer) = observed_memory().await;

    let router = Router::<MemoryBroker>::new()
        .include(forward_page)
        .publisher(MemoryPublish);
    let app = RustStream::new(AppInfo::new("rp-page", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));
    let running = app.start().await.expect("startup failed");

    ingress
        .raw(&payload(9))
        .to("rp.page.in")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "rp.page.forwarded", 9).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

// ---------------------------------------------------------------------------------------------
// Startup injections that need no attachment: a Seek parameter, single and batch.

#[subscriber(MemorySource::new("rp.seek.in"))]
async fn rewind(event: &Event, Seek(seeker): Seek<MemorySeeker>) -> HandlerResult {
    if event.id == 0 && seeker.seek(MemoryPosition::start()).await.is_err() {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_mounts_a_seek_parameter() {
    let router = Router::<MemoryBroker>::new().include(rewind);
    let app = RustStream::new(AppInfo::new("rp-seek", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .raw(&payload(1))
        .to("rp.seek.in")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("rp.seek.in")
        .assert_called_once();
}

// The harness's per-subscriber assertions ride the per-message path (the documented middleware
// exception), so a batch handler signals through a notify permit instead.
static PAGE_SEEN: Notify = Notify::const_new();

#[subscriber(batch(MemorySource::new("rp.seek.page")))]
async fn rewind_page(events: &[Event], Seek(seeker): Seek<MemorySeeker>) -> HandlerResult {
    if events.is_empty() && seeker.seek(MemoryPosition::start()).await.is_err() {
        return HandlerResult::retry();
    }
    PAGE_SEEN.notify_one();
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_mounts_a_batch_seek_parameter() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    let router = Router::<MemoryBroker>::new().include(rewind_page);
    let app = RustStream::new(AppInfo::new("rp-seek-page", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));
    let running = app.start().await.expect("startup failed");

    ingress
        .raw(&payload(1))
        .to("rp.seek.page")
        .publish()
        .await
        .expect("publish");
    PAGE_SEEN.notified().await;

    running.shutdown().await.expect("graceful shutdown failed");
}

// ---------------------------------------------------------------------------------------------
// The reply terminals: `.mount()` takes the broker's default publish policy, on the encoded and
// the byte-for-byte form alike.

#[subscriber("rp.reply.in", publish("rp.reply.out"))]
async fn relay(event: &Event) -> Event {
    Event { id: event.id + 1 }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_defaults_the_reply_publisher_on_mount() {
    let (broker, ingress, observer) = observed_memory().await;

    let router = Router::<MemoryBroker>::new().include(relay).mount();
    let app = RustStream::new(AppInfo::new("rp-reply", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));
    let running = app.start().await.expect("startup failed");

    ingress
        .raw(&payload(1))
        .to("rp.reply.in")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "rp.reply.out", 2).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

#[subscriber("rp.raw.in", raw, publish_raw("rp.raw.out"))]
async fn echo_frame(frame: &[u8]) -> Vec<u8> {
    frame.to_vec()
}

/// The byte-reply form: its reply travels a bare publisher, so it is its own route on a router.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_mounts_the_byte_reply_form() {
    let router = Router::<MemoryBroker>::new().include(echo_frame).mount();
    let app = RustStream::new(AppInfo::new("rp-raw", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .raw(b"frame")
        .to("rp.raw.in")
        .publish()
        .await
        .expect("raw publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Vec<u8>>("rp.raw.out")
        .assert_called_once()
        .with_raw(b"frame");
}

#[subscriber("rp.raw.on.in", raw, publish_raw("rp.raw.on.out"))]
async fn echo_frame_on(frame: &[u8]) -> Vec<u8> {
    frame.to_vec()
}

/// The same form with an explicit bare policy instead of the default.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_takes_an_explicit_bare_reply_policy() {
    let router = Router::<MemoryBroker>::new()
        .include(echo_frame_on)
        .publisher(MemoryPublish);
    let app = RustStream::new(AppInfo::new("rp-raw-on", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .raw(b"frame")
        .to("rp.raw.on.in")
        .publish()
        .await
        .expect("raw publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Vec<u8>>("rp.raw.on.out")
        .assert_called_once()
        .with_raw(b"frame");
}

// ---------------------------------------------------------------------------------------------
// The two-attachment forms: a reply next to Out slots, single and batch.

#[subscriber("rp.gate.in", publish("rp.gate.reply"))]
async fn gate(event: &Event, Out(out): Out<impl Publisher>) -> Result<Event, HandlerResult> {
    if out
        .raw(&payload(event.id))
        .to("rp.gate.audit")
        .publish()
        .await
        .is_err()
    {
        return Err(HandlerResult::retry());
    }
    Ok(Event { id: event.id + 1 })
}

/// The reply side defaults while the slot side is bound explicitly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_composes_a_default_reply_with_out_slots() {
    let (broker, ingress, observer) = observed_memory().await;

    let router = Router::<MemoryBroker>::new()
        .include(gate)
        .out(ruststream::runtime::DefaultSlot, MemoryPublish)
        .mount();
    let app = RustStream::new(AppInfo::new("rp-gate", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));
    let running = app.start().await.expect("startup failed");

    ingress
        .raw(&payload(7))
        .to("rp.gate.in")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "rp.gate.audit", 7).await;
    expect_id(&observer, "rp.gate.reply", 8).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

#[subscriber("rp.audit.in", raw, publish_raw("rp.audit.out"))]
async fn audited_relay(frame: &[u8], Out(audit): Out<impl Publisher>) -> Vec<u8> {
    audit
        .raw(frame)
        .to("rp.audit.copy")
        .publish()
        .await
        .expect("the slot publisher is live");
    frame.to_vec()
}

/// The byte-reply form with an `Out` slot, reply side defaulted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_composes_a_byte_reply_with_out_slots() {
    let router = Router::<MemoryBroker>::new()
        .include(audited_relay)
        .out(ruststream::runtime::DefaultSlot, MemoryPublish)
        .mount();
    let app = RustStream::new(AppInfo::new("rp-audit", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .raw(b"frame")
        .to("rp.audit.in")
        .publish()
        .await
        .expect("raw publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Vec<u8>>("rp.audit.out")
        .assert_called_once()
        .with_raw(b"frame");
    tb.broker::<MemoryBroker>()
        .published::<Vec<u8>>("rp.audit.copy")
        .assert_called_once()
        .with_raw(b"frame");
}

#[subscriber(batch("rp.ledger.in"), publish("rp.ledger.receipts"))]
async fn settle_page(
    events: &[Event],
    Out(out): Out<impl Publisher>,
) -> Result<Vec<Event>, HandlerResult> {
    let page = Event {
        id: u64::try_from(events.len()).expect("a page fits in u64"),
    };
    if out
        .raw(&serde_json::to_vec(&page).expect("serializable"))
        .to("rp.ledger.pages")
        .publish()
        .await
        .is_err()
    {
        return Err(HandlerResult::retry());
    }
    Ok(events
        .iter()
        .map(|event| Event { id: event.id + 100 })
        .collect())
}

/// The batch two-attachment form, with the reply side named explicitly this time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_composes_a_batch_reply_with_out_slots() {
    use ruststream::runtime::TypedPublisher;

    let (broker, ingress, observer) = observed_memory().await;

    let router = Router::<MemoryBroker>::new()
        .include(settle_page)
        .publisher(TypedPublisher::new(MemoryPublish))
        .out(ruststream::runtime::DefaultSlot, MemoryPublish)
        .mount();
    let app = RustStream::new(AppInfo::new("rp-ledger", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));
    let running = app.start().await.expect("startup failed");

    ingress
        .raw(&payload(7))
        .to("rp.ledger.in")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "rp.ledger.receipts", 107).await;
    expect_id(&observer, "rp.ledger.pages", 1).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

// ---------------------------------------------------------------------------------------------
// Cross-broker tokens reach a router include site, exactly as they reach a scope's.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_accepts_a_cross_broker_bind_token() {
    let egress_broker = MemoryBroker::new().bindable();
    let egress = egress_broker.bind(MemoryPublish);
    let observer = connected(egress_broker.broker()).await;

    let ingress_broker = MemoryBroker::new();
    let ingress = ingress_broker.publisher();

    // The token names its own broker, so the registration order of the two scopes is irrelevant:
    // the slot pairs against the egress broker while the subscription lives on the ingress one.
    let router = Router::<MemoryBroker>::new()
        .include(forward)
        .publisher(egress);
    let app = RustStream::new(AppInfo::new("rp-bridge", "0.1.0"))
        .with_broker(ingress_broker, |b| b.include_router(router))
        .with_broker(egress_broker, |_b| {});
    let running = app.start().await.expect("startup failed");

    ingress
        .raw(&payload(5))
        .to("rp.out.in")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "rp.out.forwarded", 5).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

// ---------------------------------------------------------------------------------------------
// The batch reply terminal, and the metadata every new route kind contributes.

#[subscriber(batch("rp.batch.in"), publish("rp.batch.out"))]
async fn bulk_relay(events: &[Event]) -> Vec<Event> {
    events
        .iter()
        .map(|event| Event { id: event.id + 1 })
        .collect()
}

/// `.mount()` on the batch publishing form takes the broker's own default publish policy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_defaults_the_batch_reply_publisher_on_mount() {
    let (broker, ingress, observer) = observed_memory().await;

    let router = Router::<MemoryBroker>::new().include(bulk_relay).mount();
    let app = RustStream::new(AppInfo::new("rp-batch", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));
    let running = app.start().await.expect("startup failed");

    ingress
        .raw(&payload(1))
        .to("rp.batch.in")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "rp.batch.out", 2).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

/// Every route kind contributes its registration metadata, in registration order: that list is
/// what the `AsyncAPI` document is generated from, so a form that mounts but stays invisible
/// there would be a silent hole.
#[test]
fn every_new_route_kind_reports_its_metadata_in_registration_order() {
    let router = Router::<MemoryBroker>::new()
        .include(rewind)
        .include(rewind_page)
        .include(echo_frame)
        .mount()
        .include(relay)
        .mount()
        .include(bulk_relay)
        .mount();

    let names: Vec<_> = router.handlers().into_iter().map(|m| m.name).collect();
    assert_eq!(
        names,
        [
            "rp.seek.in",
            "rp.seek.page",
            "rp.raw.in",
            "rp.reply.in",
            "rp.batch.in"
        ]
    );
}

// ---------------------------------------------------------------------------------------------
// The builders identify themselves while half-built: their pieces are the user's own definition
// and policies, so the name is all a `Debug` can usefully carry.

#[test]
fn the_registration_builders_name_themselves() {
    let with = Router::<MemoryBroker>::new().include(relay);
    assert!(format!("{with:?}").starts_with("RouterWith"), "{with:?}");
    let _ = with.mount();

    let slots = Router::<MemoryBroker>::new().include(transcode);
    assert!(format!("{slots:?}").starts_with("RouterSlots"), "{slots:?}");
    let _ = slots.out(Audit, MemoryPublish).out(Encoded, MemoryPublish);

    let with_reply = Router::<MemoryBroker>::new().include(gate);
    assert!(
        format!("{with_reply:?}").starts_with("RouterSlotsWithReply"),
        "{with_reply:?}"
    );
    let _ = with_reply.out(ruststream::runtime::DefaultSlot, MemoryPublish);
}
