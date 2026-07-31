//! The user-facing seek surface: a `WithSeeker` token repositioning a runtime-owned
//! subscription from outside, and a `Seek` handler parameter repositioning it from inside.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

use ruststream::memory::{
    MemoryBroker, MemoryPosition, MemoryPublish, MemoryPublisher, MemorySeeker, MemorySource,
};
use ruststream::runtime::{AppInfo, HandlerResult, Out, RustStream, Seek};
use ruststream::testing::TestApp;
use ruststream::{OutgoingMessage, Publisher, Seeker, WithSeeker, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Event {
    id: u64,
}

fn payload(id: u64) -> Vec<u8> {
    serde_json::to_vec(&Event { id }).expect("serializable")
}

#[subscriber("seek.audit")]
async fn record(_event: &Event) -> HandlerResult {
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_repositions_a_runtime_owned_subscription() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    let (source, token) = WithSeeker::attach(MemorySource::new("seek.audit"));
    let app = RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
        b.include_on(source, record);
    });
    let tb = TestApp::start(app).await.expect("harness start");

    ingress
        .publish(OutgoingMessage::new("seek.audit", payload(1).as_slice()))
        .await
        .expect("publish");
    tb.settle().await.expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("seek.audit")
        .assert_called_once();

    // Resolves once the app has started; Err before that.
    let seeker = token.seeker().expect("the subscription is open");
    seeker
        .seek(MemoryPosition::start())
        .await
        .expect("seek back");

    // The publish keeps the reaction in flight until the replay is applied, so `settle` waits
    // for both the replayed and the new delivery.
    ingress
        .publish(OutgoingMessage::new("seek.audit", payload(2).as_slice()))
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    let received: Vec<Event> = tb
        .broker::<MemoryBroker>()
        .subscriber("seek.audit")
        .received();
    let ids: Vec<u64> = received.iter().map(|event| event.id).collect();
    assert_eq!(
        ids,
        [1, 1, 2],
        "the seek must replay the first delivery before the new publish",
    );

    tb.shutdown().await.expect("graceful shutdown");
}

/// Jumps forward when the producer marks a poison region: everything queued before the
/// resume point is skipped without dropping the subscription.
#[subscriber(MemorySource::new("seek.jobs"))]
async fn work(job: &Event, Seek(seeker): Seek<MemorySeeker>) -> HandlerResult {
    if job.id == 0 {
        // The producer uses id 0 as "resume from the third message".
        if seeker.seek(MemoryPosition::sequence(2)).await.is_err() {
            return HandlerResult::retry();
        }
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seek_parameter_repositions_from_inside_the_handler() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    let app = RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| {
        b.include(work);
    });
    let tb = TestApp::start(app).await.expect("harness start");

    // All three land before the first is handled: the handler's seek must skip the queued
    // second message and resume at the third.
    for id in [0, 1, 2] {
        ingress
            .publish(OutgoingMessage::new("seek.jobs", payload(id).as_slice()))
            .await
            .expect("publish");
    }
    tb.settle().await.expect("settle");

    let received: Vec<Event> = tb
        .broker::<MemoryBroker>()
        .subscriber("seek.jobs")
        .received();
    let ids: Vec<u64> = received.iter().map(|event| event.id).collect();
    assert_eq!(
        ids,
        [0, 2],
        "the in-handler seek must skip the queued message before the target",
    );

    tb.shutdown().await.expect("graceful shutdown");
}

#[subscriber("seek.history", start_at(MemoryPosition::start()))]
async fn replayer(_event: &Event) -> HandlerResult {
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_position_replays_history_into_a_fresh_subscription() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    // Published before the app exists: only the chosen start position makes them visible.
    for id in [1, 2] {
        ingress
            .publish(OutgoingMessage::new("seek.history", payload(id).as_slice()))
            .await
            .expect("publish");
    }

    let app = RustStream::new(AppInfo::new("history", "0.1.0")).with_broker(broker, |b| {
        b.include(replayer);
    });
    let tb = TestApp::start(app).await.expect("harness start");

    // The publish keeps the reaction in flight until the startup replay is applied, so
    // `settle` waits for the history too.
    ingress
        .publish(OutgoingMessage::new("seek.history", payload(3).as_slice()))
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    let received: Vec<Event> = tb
        .broker::<MemoryBroker>()
        .subscriber("seek.history")
        .received();
    let ids: Vec<u64> = received.iter().map(|event| event.id).collect();
    assert_eq!(
        ids,
        [1, 2, 3],
        "a start position must replay pre-subscription history in order",
    );

    tb.shutdown().await.expect("graceful shutdown");
}

/// Forwards good events through the injected publisher and skips a poison region through the
/// injected seeker: the two startup injections compose in one handler.
#[subscriber("seek.combo")]
async fn forward_skipping(
    event: &Event,
    Out(out): Out<MemoryPublisher>,
    Seek(seeker): Seek<MemorySeeker>,
) -> HandlerResult {
    if event.id == 0 {
        // The poison marker: resume from the third message.
        if seeker.seek(MemoryPosition::sequence(2)).await.is_err() {
            return HandlerResult::retry();
        }
        return HandlerResult::Ack;
    }
    let payload = serde_json::to_vec(event).expect("serializable");
    if out
        .publish(OutgoingMessage::new("seek.combo.out", payload.as_slice()))
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_out_and_a_seek_parameter_combine_in_one_handler() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    let app = RustStream::new(AppInfo::new("combo", "0.1.0")).with_broker(broker, |b| {
        b.include(forward_skipping).publisher(MemoryPublish);
    });
    let tb = TestApp::start(app).await.expect("harness start");

    // All three land before the first is handled: the seek skips the queued second message,
    // and only the third reaches the forwarding branch.
    for id in [0, 1, 2] {
        ingress
            .publish(OutgoingMessage::new("seek.combo", payload(id).as_slice()))
            .await
            .expect("publish");
    }
    tb.settle().await.expect("settle");

    let received: Vec<Event> = tb
        .broker::<MemoryBroker>()
        .subscriber("seek.combo")
        .received();
    let ids: Vec<u64> = received.iter().map(|event| event.id).collect();
    assert_eq!(ids, [0, 2]);
    tb.broker::<MemoryBroker>()
        .published::<Event>("seek.combo.out")
        .assert_called_once()
        .with(&Event { id: 2 });

    tb.shutdown().await.expect("graceful shutdown");
}

/// A raw handler with an injected seeker: the input axis lets the byte-level form compose
/// with startup injections, borrowing the payload with no decode and no copy.
#[subscriber("seek.frames", raw)]
async fn raw_work(frame: &[u8], Seek(seeker): Seek<MemorySeeker>) -> HandlerResult {
    if frame == b"poison" {
        // The marker frame: resume from the third entry.
        if seeker.seek(MemoryPosition::sequence(2)).await.is_err() {
            return HandlerResult::retry();
        }
        return HandlerResult::Ack;
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_raw_handler_composes_with_a_seek_parameter() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    let app = RustStream::new(AppInfo::new("frames", "0.1.0")).with_broker(broker, |b| {
        b.include(raw_work);
    });
    let tb = TestApp::start(app).await.expect("harness start");

    for payload in [b"poison".as_slice(), b"skipped", b"kept"] {
        ingress
            .publish(OutgoingMessage::new("seek.frames", payload))
            .await
            .expect("publish");
    }
    tb.settle().await.expect("settle");

    let received = tb
        .broker::<MemoryBroker>()
        .subscriber("seek.frames")
        .received_raw();
    let frames: Vec<&[u8]> = received.iter().map(AsRef::as_ref).collect();
    assert_eq!(
        frames,
        [b"poison".as_slice(), b"kept"],
        "the raw handler's seek must skip the queued frame before the target",
    );

    tb.shutdown().await.expect("graceful shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_is_pending_before_startup() {
    let (_source, token) = WithSeeker::attach(MemorySource::new("seek.unopened"));
    let token: ruststream::SeekerToken<MemorySeeker> = token;
    assert!(
        token.seeker().is_err(),
        "a token must not resolve before the runtime opens the subscription",
    );
}

// Observed through statics: batch handlers bypass the per-message instrumentation the
// TestApp assertions ride (the documented middleware exception), so the handler records
// itself and signals through a notify permit.
static PAGE_IDS: Mutex<Vec<u64>> = Mutex::new(Vec::new());
static REPLAYED: AtomicBool = AtomicBool::new(false);
static REPLAY_DONE: Notify = Notify::const_new();

/// A batch handler with an injected seeker: the batch form composes with startup injections.
/// The tail marker (id 2) triggers one replay of the log from the second entry on; the guard
/// keeps the redelivered marker from seeking again.
#[subscriber(batch("seek.pages"))]
async fn page_work(events: &[Event], Seek(seeker): Seek<MemorySeeker>) -> HandlerResult {
    let seen_twice = {
        let mut ids = PAGE_IDS.lock().unwrap();
        ids.extend(events.iter().map(|event| event.id));
        ids.iter().filter(|id| **id == 2).count() == 2
    };
    if events.iter().any(|event| event.id == 2)
        && !REPLAYED.swap(true, Ordering::SeqCst)
        && seeker.seek(MemoryPosition::sequence(1)).await.is_err()
    {
        return HandlerResult::retry();
    }
    if seen_twice {
        REPLAY_DONE.notify_one();
    }
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_handler_composes_with_a_seek_parameter() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    let app = RustStream::new(AppInfo::new("pages", "0.1.0")).with_broker(broker, |b| {
        b.include_batch(page_work);
    });
    let running = app.start().await.expect("startup failed");

    for id in [0u64, 1, 2] {
        ingress
            .publish(OutgoingMessage::new("seek.pages", payload(id).as_slice()))
            .await
            .expect("publish");
    }
    REPLAY_DONE.notified().await;

    // However the pages split, the first pass is the publish order and the replay redelivers
    // exactly the suffix from the seek target on.
    assert_eq!(
        *PAGE_IDS.lock().unwrap(),
        [0, 1, 2, 1, 2],
        "the batch handler's seek must replay the log suffix from the target",
    );

    running.shutdown().await.expect("graceful shutdown failed");
}
