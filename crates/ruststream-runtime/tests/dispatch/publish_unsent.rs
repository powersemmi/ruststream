//! A publish the handler body makes and the broker refuses: the error carries the message the
//! broker did not send, and the handler sends it on through another `Out` as it was.

// A test drives the app on its own task; the app's cold path carries no Send.
#![allow(clippy::future_not_send)]
#![cfg(all(feature = "json", feature = "tokio"))]

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;

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
