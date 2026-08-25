//! Subscriber clause values sourced from constants and statics rather than literals: the
//! workers count (a `static usize`), the reply destination and a dictionary channel
//! (`const &str`), and a failure policy (`const FailurePolicy`).
//!
//! The dictionary channel belongs to the deprecated name-carrying `#[publishes(..)]` form,
//! which this file keeps under test: a destination declared on the message type is a literal.

#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]
#![allow(deprecated)]

use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, FailurePolicy, HandlerResult, Out, RustStream};
use ruststream::testing::TestApp;
use ruststream::{Message, OutSlot, Outgoing, Publisher, subscriber};
use serde::{Deserialize, Serialize};

static WORKERS: usize = 2;
const REPLY_TOPIC: &str = "params.replies";
const PROGRESS_CHANNEL: &str = "params.progress";
const ON_DECODE: FailurePolicy = FailurePolicy::Skip;

#[derive(Outgoing, Serialize, Deserialize, Debug, PartialEq)]
struct Ping {
    id: u64,
}

#[derive(Message, Serialize, Deserialize, Debug, PartialEq)]
struct Pong {
    id: u64,
}

#[derive(Message, Serialize, Deserialize, Debug, PartialEq)]
struct Progress {
    percent: u8,
}

#[derive(OutSlot)]
#[publishes(Progress = PROGRESS_CHANNEL)]
struct Events;

#[subscriber(
    "params.pings",
    publish(REPLY_TOPIC),
    workers(WORKERS),
    on_failure(decode = ON_DECODE)
)]
async fn respond(ping: &Ping, Out(events): Out<impl Publisher, Events, Progress>) -> Pong {
    let _ = events.publish_typed(&Progress { percent: 10 }).await;
    Pong { id: ping.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clause_values_come_from_constants_and_statics() {
    let app =
        RustStream::new(AppInfo::new("params", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(respond).out(Events, MemoryPublish).mount();
        });
    let tb = TestApp::start(app).await.expect("start");
    let broker = tb.broker::<MemoryBroker>();

    broker
        .message(&Ping { id: 7 })
        .to("params.pings")
        .publish()
        .await
        .expect("publish");

    // The reply went to the const destination, the dictionary publish to the const channel.
    broker
        .published::<Pong>(REPLY_TOPIC)
        .assert_called_once()
        .with(&Pong { id: 7 });
    broker
        .published::<Progress>(PROGRESS_CHANNEL)
        .assert_called_once()
        .with(&Progress { percent: 10 });

    // The const decode policy applies: an undecodable payload is acked past (Skip), body never
    // runs.
    broker
        .raw(b"\x00")
        .to("params.pings")
        .publish()
        .await
        .expect("publish");
    broker
        .subscriber("params.pings")
        .assert_called(2)
        .settled(HandlerResult::Ack);
    broker.published::<Pong>(REPLY_TOPIC).assert_called_once();
}
