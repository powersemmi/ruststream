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

use common::{Event, Wire, connected, expect_id, observed_memory};

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;

/// The payload view the byte-level bodies below take: the delivery's bytes, borrowed.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// The reply those bodies return: its bytes leave on the wire as they are.
#[derive(Serialized)]
struct Export(Vec<u8>);

// ---------------------------------------------------------------------------------------------
// Out slots: the single-slot shorthand, named slots, and the batch counterpart.

#[subscriber("rp.out.in")]
async fn forward(event: &Event, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    if out
        .message(event)
        .to("rp.out.forwarded")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The one-slot shorthand on a router: `.publisher(policy)` binds the slot and commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_mounts_a_single_out_slot() {
    let (broker, ingress, observer) = observed_memory().await;

    let router = Router::<MemoryBroker>::new()
        .include(forward)
        .publisher(Publish)
        .build();
    let app = RustStream::new(AppInfo::new("rp-out", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));
    let running = app.start().await.expect("startup failed");

    ingress
        .message(&Event { id: 3 })
        .to("rp.out.in")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "rp.out.forwarded", 3).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

#[derive(OutSlot)]
#[publishes(Wire)]
struct Encoded;

#[derive(OutSlot)]
#[publishes(Wire)]
struct Audit;

#[subscriber("rp.slots.in")]
async fn transcode(
    chunk: &Frame<'_>,
    Out(encoded): Out<impl Publisher, Encoded>,
    Out(audit): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    if encoded
        .message(&Wire::of(chunk.0))
        .to("rp.slots.encoded")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    let receipt = chunk.0.len().to_be_bytes();
    if audit
        .message(&Wire::of(receipt))
        .to("rp.slots.audit")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// Named slots bind by marker in any order, and `.build()` is the terminal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_binds_named_out_slots_by_marker() {
    // Deliberately bound in the opposite of the signature order.
    let router = Router::<MemoryBroker>::new()
        .include(transcode)
        .out(Audit, Publish)
        .out(Encoded, Publish)
        .build();
    let app = RustStream::new(AppInfo::new("rp-slots", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Wire::of(b"frame"))
        .to("rp.slots.in")
        .publish()
        .await
        .expect("publish");

    tb.out::<Encoded>().assert_called_once().with_raw(b"frame");
    tb.out::<Audit>().assert_called_once();
}

/// A serialized dictionary member the slot's typed entry publishes byte-for-byte, the scope
/// counterpart being `tests/lanes.rs`.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "rp.wire.out")]
struct WireCopy(Vec<u8>);

#[derive(OutSlot)]
#[publishes(WireCopy)]
struct Wires;

#[subscriber("rp.wire.in")]
async fn copy_out(frame: &Frame<'_>, Out(out): Out<impl Publisher, Wires>) -> HandlerOutcome {
    if out
        .message(&WireCopy(frame.0.to_vec()))
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The serialized wire of a slot's typed entry mounts from a router exactly as from the scope:
/// the bytes leave as they are, at the destination the type declares.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_publishes_a_serialized_message_through_a_slot() {
    let router = Router::<MemoryBroker>::new()
        .include(copy_out)
        .out(Wires, Publish)
        .build();
    let app = RustStream::new(AppInfo::new("rp-wire", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Wire::of(b"frame"))
        .to("rp.wire.in")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.out::<Wires>().assert_called_once().with_raw(b"frame");
    tb.broker::<MemoryBroker>()
        .published::<WireCopy>("rp.wire.out")
        .assert_called_once()
        .with_raw(b"frame");
}

#[subscriber("rp.page.in")]
async fn forward_page(events: &[Event], Out(out): Out<impl Publisher>) -> HandlerOutcome {
    for event in events {
        if out
            .message(event)
            .to("rp.page.forwarded")
            .publish()
            .await
            .is_err()
        {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_mounts_a_batch_out_slot() {
    let (broker, ingress, observer) = observed_memory().await;

    let router = Router::<MemoryBroker>::new()
        .include(forward_page.batch(nonzero!(64)))
        .publisher(Publish)
        .build();
    let app = RustStream::new(AppInfo::new("rp-page", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));
    let running = app.start().await.expect("startup failed");

    ingress
        .message(&Event { id: 9 })
        .to("rp.page.in")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "rp.page.forwarded", 9).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

// ---------------------------------------------------------------------------------------------
// A broker context key: the seek handle rides the delivery context, read by the Ctx extractor.

#[subscriber(MemorySource::new("rp.seek.in"))]
async fn rewind(event: &Event, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
    if event.id == 0 && seeker.seek(MemoryPosition::start()).await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_mounts_a_seek_key_reader() {
    let router = Router::<MemoryBroker>::new().include(rewind);
    let app = RustStream::new(AppInfo::new("rp-seek", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Event { id: 1 })
        .to("rp.seek.in")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("rp.seek.in")
        .assert_called_once();
}

// ---------------------------------------------------------------------------------------------
// The reply terminals: `.build()` takes the broker's default publish policy, on the encoded and
// the byte-for-byte form alike.

#[subscriber("rp.reply.in", publish("rp.reply.out"))]
async fn relay(event: &Event) -> Event {
    Event { id: event.id + 1 }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_defaults_the_reply_publisher_on_mount() {
    let (broker, ingress, observer) = observed_memory().await;

    let router = Router::<MemoryBroker>::new().include(relay).build();
    let app = RustStream::new(AppInfo::new("rp-reply", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));
    let running = app.start().await.expect("startup failed");

    ingress
        .message(&Event { id: 1 })
        .to("rp.reply.in")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "rp.reply.out", 2).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

#[subscriber("rp.raw.in", publish("rp.raw.out"))]
async fn echo_frame(frame: &Frame<'_>) -> Export {
    Export(frame.0.to_vec())
}

/// The byte-reply form: its reply leaves unencoded, so it is its own route on a router.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_mounts_the_byte_reply_form() {
    let router = Router::<MemoryBroker>::new().include(echo_frame).build();
    let app = RustStream::new(AppInfo::new("rp-raw", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Wire::of(b"frame"))
        .to("rp.raw.in")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Export>("rp.raw.out")
        .assert_called_once()
        .with_raw(b"frame");
}

#[subscriber("rp.raw.on.in", publish("rp.raw.on.out"))]
async fn echo_frame_on(frame: &Frame<'_>) -> Export {
    Export(frame.0.to_vec())
}

/// The same form with an explicit publish policy instead of the broker default.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_takes_an_explicit_serialized_reply_policy() {
    let router = Router::<MemoryBroker>::new()
        .include(echo_frame_on)
        .publisher(Publish)
        .build();
    let app = RustStream::new(AppInfo::new("rp-raw-on", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Wire::of(b"frame"))
        .to("rp.raw.on.in")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Export>("rp.raw.on.out")
        .assert_called_once()
        .with_raw(b"frame");
}

// ---------------------------------------------------------------------------------------------
// The two-attachment forms: a reply next to Out slots, single and batch.

#[subscriber("rp.gate.in", publish("rp.gate.reply"))]
async fn gate(event: &Event, Out(out): Out<impl Publisher>) -> Result<Event, HandlerOutcome> {
    if out
        .message(event)
        .to("rp.gate.audit")
        .publish()
        .await
        .is_err()
    {
        return Err(HandlerOutcome::retry());
    }
    Ok(Event { id: event.id + 1 })
}

/// The reply side defaults while the slot side is bound explicitly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_composes_a_default_reply_with_out_slots() {
    let (broker, ingress, observer) = observed_memory().await;

    let router = Router::<MemoryBroker>::new()
        .include(gate)
        .out(DefaultSlot, Publish)
        .build();
    let app = RustStream::new(AppInfo::new("rp-gate", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));
    let running = app.start().await.expect("startup failed");

    ingress
        .message(&Event { id: 7 })
        .to("rp.gate.in")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "rp.gate.audit", 7).await;
    expect_id(&observer, "rp.gate.reply", 8).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

#[subscriber("rp.audit.in", publish("rp.audit.out"))]
async fn audited_relay(frame: &Frame<'_>, Out(audit): Out<impl Publisher>) -> Export {
    audit
        .message(&Wire::of(frame.0))
        .to("rp.audit.copy")
        .publish()
        .await
        .expect("the slot publisher is live");
    Export(frame.0.to_vec())
}

/// The byte-reply form with an `Out` slot, reply side defaulted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_composes_a_byte_reply_with_out_slots() {
    let router = Router::<MemoryBroker>::new()
        .include(audited_relay)
        .out(DefaultSlot, Publish)
        .build();
    let app = RustStream::new(AppInfo::new("rp-audit", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Wire::of(b"frame"))
        .to("rp.audit.in")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Export>("rp.audit.out")
        .assert_called_once()
        .with_raw(b"frame");
    tb.broker::<MemoryBroker>()
        .published::<Export>("rp.audit.copy")
        .assert_called_once()
        .with_raw(b"frame");
}

#[subscriber("rp.ledger.in", publish("rp.ledger.receipts"))]
async fn settle_page(
    events: &[Event],
    Out(out): Out<impl Publisher>,
) -> Result<Vec<Event>, HandlerOutcome> {
    let page = Event {
        id: u64::try_from(events.len()).expect("a page fits in u64"),
    };
    if out
        .message(&page)
        .to("rp.ledger.pages")
        .publish()
        .await
        .is_err()
    {
        return Err(HandlerOutcome::retry());
    }
    Ok(events
        .iter()
        .map(|event| Event { id: event.id + 100 })
        .collect())
}

/// The batch two-attachment form, with the reply side named explicitly this time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_composes_a_batch_reply_with_out_slots() {
    let (broker, ingress, observer) = observed_memory().await;

    let router = Router::<MemoryBroker>::new()
        .include(settle_page.batch(nonzero!(64)))
        .publisher(Publish)
        .out(DefaultSlot, Publish)
        .build();
    let app = RustStream::new(AppInfo::new("rp-ledger", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));
    let running = app.start().await.expect("startup failed");

    ingress
        .message(&Event { id: 7 })
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
    let egress = egress_broker.bind(Publish);
    let observer = connected(egress_broker.broker()).await;

    let ingress_broker = MemoryBroker::new();
    let ingress = ingress_broker.publisher();

    // The token names its own broker, so the registration order of the two scopes is irrelevant:
    // the slot pairs against the egress broker while the subscription lives on the ingress one.
    let router = Router::<MemoryBroker>::new()
        .include(forward)
        .publisher(egress)
        .build();
    let app = RustStream::new(AppInfo::new("rp-bridge", "0.1.0"))
        .with_broker(ingress_broker, |b| b.include_router(router))
        .with_broker(egress_broker, |_b| {});
    let running = app.start().await.expect("startup failed");

    ingress
        .message(&Event { id: 5 })
        .to("rp.out.in")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "rp.out.forwarded", 5).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

// ---------------------------------------------------------------------------------------------
// The batch reply terminal, and the metadata every new route kind contributes.

#[subscriber("rp.batch.in", publish("rp.batch.out"))]
async fn bulk_relay(events: &[Event]) -> Vec<Event> {
    events
        .iter()
        .map(|event| Event { id: event.id + 1 })
        .collect()
}

/// A batch publishing form mounted with only its page size takes the broker's own default
/// publish policy: naming the size seals the definition, so the reply wiring defaults.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_defaults_the_batch_reply_publisher_on_mount() {
    let (broker, ingress, observer) = observed_memory().await;

    let router = Router::<MemoryBroker>::new().include(bulk_relay.batch(nonzero!(64)));
    let app = RustStream::new(AppInfo::new("rp-batch", "0.1.0"))
        .with_broker(broker, |b| b.include_router(router));
    let running = app.start().await.expect("startup failed");

    ingress
        .message(&Event { id: 1 })
        .to("rp.batch.in")
        .publish()
        .await
        .expect("publish");
    expect_id(&observer, "rp.batch.out", 2).await;

    running.shutdown().await.expect("graceful shutdown failed");
}

/// Every route kind contributes its registration metadata, in registration order: that list is
/// what the `AsyncAPI` document is generated from.
#[test]
fn every_new_route_kind_reports_its_metadata_in_registration_order() {
    let router = Router::<MemoryBroker>::new()
        .include(rewind)
        .include(echo_frame)
        .build()
        .include(relay)
        .build()
        .include(bulk_relay.batch(nonzero!(64)));

    let names: Vec<_> = router.handlers().into_iter().map(|m| m.name).collect();
    assert_eq!(
        names,
        ["rp.seek.in", "rp.raw.in", "rp.reply.in", "rp.batch.in"]
    );
}

// ---------------------------------------------------------------------------------------------
// The builders identify themselves by name while half-built.

#[test]
fn the_registration_builders_name_themselves() {
    let with = Router::<MemoryBroker>::new().include(relay);
    assert!(format!("{with:?}").starts_with("RouterWith"), "{with:?}");
    let _ = with.build();

    let slots = Router::<MemoryBroker>::new().include(transcode);
    assert!(format!("{slots:?}").starts_with("RouterSlots"), "{slots:?}");
    let _ = slots.out(Audit, Publish).out(Encoded, Publish);

    let with_reply = Router::<MemoryBroker>::new().include(gate);
    assert!(
        format!("{with_reply:?}").starts_with("RouterSlotsWithReply"),
        "{with_reply:?}"
    );
    let _ = with_reply.out(DefaultSlot, Publish);
}
