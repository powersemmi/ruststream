//! The user-facing seek surface: a `WithSeeker` token repositioning a runtime-owned
//! subscription from outside, and a `Seek` handler parameter repositioning it from inside.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

use ruststream::memory::{MemoryBroker, MemoryPosition, MemorySeeker, MemorySource};
use ruststream::runtime::{AppInfo, HandlerResult, RustStream, Seek};
use ruststream::testing::TestApp;
use ruststream::{OutgoingMessage, Publisher, Seeker, StartAt, WithSeeker, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
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

#[subscriber("seek.history")]
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
        b.include_on(
            StartAt::new(MemorySource::new("seek.history"), MemoryPosition::start()),
            replayer,
        );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_is_pending_before_startup() {
    let (_source, token) = WithSeeker::attach(MemorySource::new("seek.unopened"));
    let token: ruststream::SeekerToken<MemorySeeker> = token;
    assert!(
        token.seeker().is_err(),
        "a token must not resolve before the runtime opens the subscription",
    );
}
