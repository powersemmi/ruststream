//! The lanes that carry their own bytes need no codec, so they must compile in a build with no
//! codec feature at all: `Deserialized` on the way in, `Serialized` on the way out, and the typed
//! publish entry point over both.
//!
//! This file is the negative half of the codec surface. It is deliberately compiled only when no
//! codec feature is on, which is what `cargo check --no-default-features --features
//! testing,memory,macros --all-targets` (the codec-free gate) exercises.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "testing",
    not(any(feature = "json", feature = "cbor", feature = "msgpack"))
))]

use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream::{Deserialized, Serialized};

/// A self-deserializing view: the framework's codec never runs on it, so nothing here needs one.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// A self-carrying wire type, declaring where it goes.
#[derive(Outgoing, Serialized)]
#[outgoing(name = "codecfree.frames")]
struct WireFrame(Vec<u8>);

#[subscriber("codecfree.frames")]
async fn ingest(frame: &Frame<'_>) -> HandlerOutcome {
    let _ = frame.0.len();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_self_carrying_lanes_run_without_a_codec_feature() {
    let app =
        RustStream::new(AppInfo::new("codecfree", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(ingest);
        });
    let tb = TestApp::start(app).await.expect("harness start");

    // The typed entry point exists with no codec feature; this value asks nothing of the codec
    // position, so the publish resolves and the bytes leave as they are.
    tb.message(&WireFrame(vec![1, 2, 3]))
        .publish()
        .await
        .expect("inject");

    tb.broker::<MemoryBroker>()
        .subscriber("codecfree.frames")
        .assert_called_once()
        .with_raw(&[1, 2, 3])
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("graceful shutdown");
}
