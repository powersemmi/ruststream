//! The type-selected pipeline lanes end to end: `Deserialize`/`Serialize` mean the framework's
//! codec does the work, `Deserialized`/`Serialized` mean the user's own type already did it -
//! and one signature mixes the lanes freely, because each end picks its own.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};

/// A self-deserializing view over the payload: the framework's codec never runs on this lane.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// A self-serialized reply: its bytes leave exactly as returned, with no codec.
#[derive(Serialized)]
struct Export(Vec<u8>);

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema, PartialEq)]
struct Report {
    len: usize,
}

/// Raw in, raw out: both lanes are the user's own.
#[subscriber("lanes.mirror", publish("lanes.mirror.out"))]
async fn mirror(frame: &Frame<'_>) -> Export {
    Export(frame.0.to_vec())
}

/// Raw in, encoded out: a `Deserialized` input composes with a `Serialize` reply - the input
/// lane skips the codec, the reply still rides it.
#[subscriber("lanes.measure", publish("lanes.measure.out"))]
async fn measure(frame: &Frame<'_>) -> Report {
    Report { len: frame.0.len() }
}

/// Decoded in, raw out: the gateway shape - the input decodes with the scope codec, the reply
/// leaves byte-for-byte.
#[subscriber("lanes.encode", publish("lanes.encode.out"))]
async fn encode(report: &Report) -> Export {
    Export(report.len.to_be_bytes().to_vec())
}

/// A page of self-deserializing views.
#[subscriber("lanes.frames")]
async fn ingest(frames: &[Frame<'_>]) -> HandlerOutcome {
    let _ = frames.iter().map(|frame| frame.0.len()).sum::<usize>();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_lanes_compose_end_to_end() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    let app = RustStream::new(AppInfo::new("lanes", "0.1.0")).with_broker(broker, |b| {
        b.include(mirror);
        b.include(measure);
        b.include(encode);
        b.include(ingest);
    });
    let tb = TestApp::start(app).await.expect("harness start");

    ingress
        .raw(b"\x00\x01\x02".as_slice())
        .to("lanes.mirror")
        .publish()
        .await
        .expect("publish");
    ingress
        .raw(b"four".as_slice())
        .to("lanes.measure")
        .publish()
        .await
        .expect("publish");
    ingress
        .raw(br#"{"len":7}"#.as_slice())
        .to("lanes.encode")
        .publish()
        .await
        .expect("publish");
    ingress
        .raw(b"page".as_slice())
        .to("lanes.frames")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    // The serialized reply is the input's bytes, untouched by any codec.
    tb.broker::<MemoryBroker>()
        .published::<Vec<u8>>("lanes.mirror.out")
        .assert_called_once()
        .with_raw(b"\x00\x01\x02");
    // The encoded reply of a raw input still rides the scope codec.
    tb.broker::<MemoryBroker>()
        .published::<Report>("lanes.measure.out")
        .assert_called_once()
        .with(&Report { len: 4 });
    // The gateway shape: decoded input, byte reply.
    tb.broker::<MemoryBroker>()
        .published::<Vec<u8>>("lanes.encode.out")
        .assert_called_once()
        .with_raw(&7usize.to_be_bytes());

    tb.shutdown().await.expect("graceful shutdown");
}

/// The dictionary side of a serialized out type: it declares its destination and slot
/// membership like any model, and the handler publishes its bytes through the slot.
#[derive(Outgoing)]
#[outgoing(name = "lanes.exports")]
struct WireExport(Vec<u8>);

impl Serialized for WireExport {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(OutSlot)]
#[publishes(WireExport)]
struct Exports;

/// A serialized dictionary member rides the slot's raw builder; the declared set still shows
/// the type in the generated document under its own name.
#[subscriber("lanes.chunks")]
async fn export(frame: &Frame<'_>, Out(out): Out<impl Publisher, Exports>) -> HandlerOutcome {
    let wire = WireExport(frame.0.to_vec());
    if out
        .raw(wire.bytes())
        .to("lanes.exports")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_serialized_out_type_is_a_dictionary_member() {
    let broker = MemoryBroker::new();
    let ingress = broker.publisher();

    let app = RustStream::new(AppInfo::new("exports", "0.1.0")).with_broker(broker, |b| {
        b.include(export).publisher(MemoryPublish);
    });
    let tb = TestApp::start(app).await.expect("harness start");

    ingress
        .raw(b"chunk".as_slice())
        .to("lanes.chunks")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.broker::<MemoryBroker>()
        .published::<Vec<u8>>("lanes.exports")
        .assert_called_once()
        .with_raw(b"chunk");

    tb.shutdown().await.expect("graceful shutdown");
}
