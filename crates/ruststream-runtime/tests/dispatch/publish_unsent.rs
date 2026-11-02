//! A publish the handler body makes and the broker refuses: the error carries the message the
//! broker did not send, and the handler sends it on through another `Out` as it was.

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
#![cfg(all(feature = "json", feature = "tokio"))]

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use std::error::Error;

use ruststream_runtime::{Outgoing, PublishLayer, PublishNext, PublishPipeline, SlotContext};

use super::error::{DownPublish, Job};

#[derive(OutSlot)]
#[publishes(Job)]
struct Primary;

/// Publishes through a broker that is down, and on a refusal republishes the message the error
/// returned through the other `Out`.
#[subscriber("relay")]
async fn relay(
    job: &Job,
    Out(primary): Out<impl Publisher, Primary>,
    Out(fallback): Out<impl Publisher>,
) -> HandlerOutcome {
    if let Err(refused) = primary.message(job).to("primary").publish().await {
        let unsent = refused.unsent().expect("the refusal returns the message");
        fallback
            .message(unsent)
            .to("fallback")
            .publish()
            .await
            .expect("the fallback takes the message");
    }
    HandlerOutcome::ack()
}

#[tokio::test(start_paused = true)]
async fn a_refused_publish_returns_the_message_for_another_out() {
    let app =
        RustStream::new(AppInfo::new("relay", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(relay)
                .out(Primary, DownPublish)
                .out(DefaultSlot, Publish)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 9 })
        .to("relay")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Job>("fallback")
        .assert_called_once()
        .with(&Job { id: 9 });
}

/// A slot transform that stamps a header and rewrites the payload into a buffer of the call.
#[derive(Clone)]
struct Rewrite;

impl<Options> PublishTransform<ForSlot, Options> for Rewrite {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &SlotContext<'_>) {
        out.headers_mut().insert("x-stamp", "yes");
        out.set_payload(&b"rewritten"[..]);
    }
}

/// Republishes on a refusal what the error returned, headers included.
#[subscriber("relay-rewritten")]
async fn relay_rewritten(
    job: &Job,
    Out(primary): Out<impl Publisher, Primary>,
    Out(fallback): Out<impl Publisher>,
) -> HandlerOutcome {
    if let Err(refused) = primary.message(job).to("primary").publish().await {
        let unsent = refused.unsent().expect("the refusal returns the message");
        fallback
            .message(unsent)
            .to("fallback")
            .with_headers(unsent.headers().clone())
            .publish()
            .await
            .expect("the fallback takes the message");
    }
    HandlerOutcome::ack()
}

/// The message a slot's transforms rewrote is the one the error returns: its bytes and the header
/// the transform wrote, not the message the handler built.
#[tokio::test(start_paused = true)]
async fn a_refused_publish_through_slot_transforms_returns_the_rewritten_message() {
    let app =
        RustStream::new(AppInfo::new("relay", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(relay_rewritten)
                .out(Primary, DownPublish)
                .transform(Rewrite)
                .out(DefaultSlot, Publish)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 9 })
        .to("relay-rewritten")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Job>("fallback")
        .assert_called_once()
        .with_raw(b"rewritten")
        .with_header("x-stamp", "yes");
}

/// A message that carries its own bytes, which leave as they are.
#[derive(Outgoing, Serialized)]
struct Blob(Vec<u8>);

#[derive(OutSlot)]
#[publishes(Blob)]
struct Blobs;

/// Publishes the bytes a value carries and, on a refusal, sends the returned message on.
#[subscriber("relay-blob")]
async fn relay_blob(
    _job: &Job,
    Out(primary): Out<impl Publisher, Blobs>,
    Out(fallback): Out<impl Publisher>,
) -> HandlerOutcome {
    let blob = Blob(b"own bytes".to_vec());
    if let Err(refused) = primary.message(&blob).to("primary").publish().await {
        let unsent = refused.unsent().expect("the refusal returns the message");
        fallback
            .message(unsent)
            .to("fallback")
            .publish()
            .await
            .expect("the fallback takes the message");
    }
    HandlerOutcome::ack()
}

/// The bytes a value holds come back with the refusal, as the value produced them.
#[tokio::test(start_paused = true)]
async fn a_refused_publish_of_a_value_s_own_bytes_returns_them() {
    let app =
        RustStream::new(AppInfo::new("relay", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(relay_blob)
                .out(Blobs, DownPublish)
                .out(DefaultSlot, Publish)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 1 })
        .to("relay-blob")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Job>("fallback")
        .assert_called_once()
        .with_raw(b"own bytes");
}

/// An app-wide layer that stamps and rewrites what leaves for the primary destination only, so the
/// fallback shows what the refused send carried.
#[derive(Clone)]
struct StampPrimary;

impl PublishLayer for StampPrimary {
    async fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'_>,
        next: PublishNext<'a, N, P>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if out.name() == "primary" {
            out.headers_mut().insert("x-layer", "yes");
            out.set_payload(&b"layered"[..]);
        }
        next.run(out).await
    }
}

/// The message the app's publish layers rewrote is the one the error returns.
#[tokio::test(start_paused = true)]
async fn a_refused_publish_under_a_publish_layer_returns_the_message_it_left() {
    let app = RustStream::new(AppInfo::new("relay", "0.1.0"))
        .publish_layer(StampPrimary)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(relay_rewritten)
                .out(Primary, DownPublish)
                .out(DefaultSlot, Publish)
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Job { id: 9 })
        .to("relay-rewritten")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Job>("fallback")
        .assert_called_once()
        .with_raw(b"layered")
        .with_header("x-layer", "yes");
}
