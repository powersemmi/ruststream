//! The user-facing seek surface: the broker's context keys repositioning the subscription
//! from inside a delivery, and a `start_at(..)` clause choosing where it opens.
//!
//! The position and the reposition handle are ordinary broker context fields: the in-memory
//! broker publishes the `SeekHandle` / `Position` keys over its [`MemoryContext`], and a
//! handler reads them with the `Ctx` extractor like any other broker field.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

mod common;

use tokio::sync::Notify;

use ruststream::memory::{MemoryBroker, MemoryPosition, MemoryPublish, SeekHandle};
use ruststream::runtime::{AppInfo, Ctx, HandlerOutcome, Out, PublishExt, RustStream};
use ruststream::testing::TestApp;
use ruststream::{Deserialized, Publisher, Seeker, subscriber};

use common::{Event, Wire};

/// The payload view the byte-level handler below takes: the delivery's bytes, borrowed.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// Jumps forward when the producer marks a poison region: everything queued before the
/// resume point is skipped without dropping the subscription.
// A forward seek only skips what is already in the log, so every queued-skip test holds its
// seeking handler on a permit until the producer has published the whole run; without the
// gate, a fast dispatcher can handle the marker before the later entries land and the
// end-clamped seek becomes a no-op (a real CI flake).
static JOBS_PUBLISHED: Notify = Notify::const_new();

#[subscriber("seek.jobs")]
async fn work(job: &Event, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
    if job.id == 0 {
        // The producer uses id 0 as "resume from the third message".
        JOBS_PUBLISHED.notified().await;
        if seeker.seek(MemoryPosition::sequence(2)).await.is_err() {
            return HandlerOutcome::retry();
        }
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seek_key_repositions_from_inside_the_handler() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    let app = RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| {
        b.include(work);
    });
    let tb = TestApp::start(app).await.expect("harness start");

    // The marker's seek waits for the whole run to land, then must skip the queued second
    // message and resume at the third.
    for id in [0, 1, 2] {
        ingress
            .message(&Event { id })
            .to("seek.jobs")
            .publish()
            .await
            .expect("publish");
    }
    JOBS_PUBLISHED.notify_one();
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
async fn replayer(_event: &Event) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_position_replays_history_into_a_fresh_subscription() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    // Published before the app exists: only the chosen start position makes them visible.
    for id in [1, 2] {
        ingress
            .message(&Event { id })
            .to("seek.history")
            .publish()
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
        .message(&Event { id: 3 })
        .to("seek.history")
        .publish()
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

static COMBO_PUBLISHED: Notify = Notify::const_new();

/// Forwards good events through the injected publisher and skips a poison region through the
/// context's seek handle: a startup injection and a broker context field compose in one
/// handler.
#[subscriber("seek.combo")]
async fn forward_skipping(
    event: &Event,
    Out(out): Out<impl Publisher>,
    Ctx(seeker): Ctx<SeekHandle>,
) -> HandlerOutcome {
    if event.id == 0 {
        // The poison marker: resume from the third message once the whole run is in the log.
        COMBO_PUBLISHED.notified().await;
        if seeker.seek(MemoryPosition::sequence(2)).await.is_err() {
            return HandlerOutcome::retry();
        }
        return HandlerOutcome::ack();
    }
    if out
        .message(event)
        .to("seek.combo.out")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_out_parameter_and_a_seek_key_combine_in_one_handler() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    let app = RustStream::new(AppInfo::new("combo", "0.1.0")).with_broker(broker, |b| {
        b.include(forward_skipping).publisher(MemoryPublish);
    });
    let tb = TestApp::start(app).await.expect("harness start");

    // The marker's seek waits for the whole run: it skips the queued second message, and only
    // the third reaches the forwarding branch.
    for id in [0, 1, 2] {
        ingress
            .message(&Event { id })
            .to("seek.combo")
            .publish()
            .await
            .expect("publish");
    }
    COMBO_PUBLISHED.notify_one();
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

static FRAMES_PUBLISHED: Notify = Notify::const_new();

/// A raw handler with the seek key: the input axis lets the byte-level form compose with a
/// broker context field, borrowing the payload with no decode and no copy.
#[subscriber("seek.frames")]
async fn raw_work(frame: &Frame<'_>, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
    if frame.0 == b"poison" {
        // The marker frame: resume from the third entry once the whole run is in the log.
        FRAMES_PUBLISHED.notified().await;
        if seeker.seek(MemoryPosition::sequence(2)).await.is_err() {
            return HandlerOutcome::retry();
        }
        return HandlerOutcome::ack();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_raw_handler_composes_with_a_seek_key() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    let app = RustStream::new(AppInfo::new("frames", "0.1.0")).with_broker(broker, |b| {
        b.include(raw_work);
    });
    let tb = TestApp::start(app).await.expect("harness start");

    for payload in [b"poison".as_slice(), b"skipped", b"kept"] {
        ingress
            .message(&Wire::of(payload))
            .to("seek.frames")
            .publish()
            .await
            .expect("publish");
    }
    FRAMES_PUBLISHED.notify_one();
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

static GATE_PUBLISHED: Notify = Notify::const_new();

/// A publishing handler with the seek key: the reply form composes with a broker context
/// field. The poison marker skips its own reply and repositions the subscription instead;
/// only the post-seek event is answered.
#[subscriber("seek.gate", publish("seek.gate.out"))]
async fn gate(event: &Event, Ctx(seeker): Ctx<SeekHandle>) -> Result<Event, HandlerOutcome> {
    if event.id == 0 {
        // The poison marker: resume from the third message once the whole run is in the
        // log, publishing nothing.
        GATE_PUBLISHED.notified().await;
        if seeker.seek(MemoryPosition::sequence(2)).await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        return Err(HandlerOutcome::ack());
    }
    Ok(Event { id: event.id * 10 })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publishing_handler_composes_with_a_seek_key() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    let app = RustStream::new(AppInfo::new("gate", "0.1.0")).with_broker(broker, |b| {
        b.include(gate);
    });
    let tb = TestApp::start(app).await.expect("harness start");

    // The marker's seek waits for the whole run: it skips the queued second message, and
    // only the third reaches the replying branch.
    for id in [0, 1, 2] {
        ingress
            .message(&Event { id })
            .to("seek.gate")
            .publish()
            .await
            .expect("publish");
    }
    GATE_PUBLISHED.notify_one();
    tb.settle().await.expect("settle");

    let received: Vec<Event> = tb
        .broker::<MemoryBroker>()
        .subscriber("seek.gate")
        .received();
    let ids: Vec<u64> = received.iter().map(|event| event.id).collect();
    assert_eq!(ids, [0, 2]);
    tb.broker::<MemoryBroker>()
        .published::<Event>("seek.gate.out")
        .assert_called_once()
        .with(&Event { id: 20 });

    tb.shutdown().await.expect("graceful shutdown");
}
