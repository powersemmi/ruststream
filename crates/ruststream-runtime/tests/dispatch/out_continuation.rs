//! A cloned `Out` publisher moved into a continuation: the clone is made through a renamed
//! binding or a helper, and the harness's record of the slot sees what the continuation
//! publishes.

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
#![cfg(all(feature = "json", feature = "tokio"))]

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

use crate::error::{DownPublish, Job, Queue, RETRY_DELAY};

/// What the continuations raise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "alerts")]
struct Alert {
    id: u32,
}

/// The slot alerts leave through.
#[derive(OutSlot)]
#[publishes(Alert)]
struct Alerts;

/// Copies a value the body holds, so the clone is not written on the binding.
fn copy_of<Value: Clone>(value: &Value) -> Value {
    value.clone()
}

/// Raises an alert from a `.then` through an entry cloned by a helper.
#[subscriber("then-helper")]
async fn then_helper(job: &Job, ctx: &mut Context, Out(alerts): Out<impl Publisher, Alerts>) {
    let id = job.id;
    let held = copy_of(alerts);
    ctx.after(HandlerOutcome::ack()).then(async move {
        held.message(&Alert { id }).publish().await.expect("alert");
    });
}

/// Raises an alert from an `.error` through an entry moved to a new name before it is cloned.
#[subscriber(Queue { name: "error-renamed" })]
async fn error_renamed(job: &Job, Out(alerts): Out<impl Publisher, Alerts>) -> HandlerOutcome {
    let id = job.id;
    let mine = alerts;
    let held = mine.clone();
    HandlerOutcome::retry_after(RETRY_DELAY).error(async move |_failures: Failures| {
        held.message(&Alert { id }).publish().await.expect("alert");
    })
}

#[tokio::test(start_paused = true)]
async fn the_slot_record_sees_a_publish_from_a_then() {
    let app = RustStream::new(AppInfo::new("outs", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(then_helper).out(Alerts, Publish)
        });
    let tb = TestApp::start(app).await.expect("harness start");
    tb.broker::<MemoryBroker>()
        .message(&Job { id: 1 })
        .to("then-helper")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.out::<Alerts>().assert_called(1);
}

#[tokio::test(start_paused = true)]
async fn the_slot_record_sees_a_publish_from_an_error() {
    let app =
        RustStream::new(AppInfo::new("outs", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(error_renamed)
                .out(Alerts, Publish)
                .out_retry(DownPublish)
        });
    let tb = TestApp::start(app).await.expect("harness start");
    tb.broker::<MemoryBroker>()
        .message(&Job { id: 2 })
        .to("error-renamed")
        .publish()
        .await
        .expect("publish");
    tb.advance(RETRY_DELAY).await.expect("advance");

    tb.out::<Alerts>().assert_called(1);
}
