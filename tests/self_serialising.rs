//! A value that serializes itself: `#[wire(..)]` names the format's own functions, and the lane
//! carries the type with no codec between it and the wire.
//!
//! The two shapes `Serialized` covers meet here. A byte bag lends what it holds, and must keep
//! lending it - zero-copy is the property the lane was introduced for. A type that holds fields
//! writes them into the buffer the publish path already carries, so nothing intermediate is
//! allocated and the model type stays visible at the mount site.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

use std::fmt;

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;

/// A value that already holds its bytes, declaring no destination of its own.
#[derive(Outgoing, Serialized)]
struct Export(Vec<u8>);

/// The failure of the frame writer below, so the fallible half of `#[wire(encode = ..)]` is a
/// real error type and not a stand-in.
#[derive(Debug)]
struct SequenceTooHigh(u32);

impl fmt::Display for SequenceTooHigh {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sequence {} does not fit the frame", self.0)
    }
}

impl std::error::Error for SequenceTooHigh {}

/// The failure of the frame reader.
#[derive(Debug)]
struct BadFrame;

impl fmt::Display for BadFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a tick frame is four big-endian bytes")
    }
}

impl std::error::Error for BadFrame {}

/// The highest sequence the frame can carry, so the writer has something to reject.
const MAX_SEQUENCE: u32 = 0x00ff_ffff;

/// A hand-rolled frame: no generator, no generated code, and no crate the core knows about -
/// only two functions the attribute names.
#[derive(Debug, PartialEq, Outgoing, Serialized, Deserialized)]
#[outgoing(name = "wire.ticks")]
#[wire(encode = write_tick, decode = read_tick)]
struct Tick {
    seq: u32,
}

fn write_tick(tick: &Tick, buf: &mut BytesMut) -> Result<(), SequenceTooHigh> {
    if tick.seq > MAX_SEQUENCE {
        return Err(SequenceTooHigh(tick.seq));
    }
    buf.extend_from_slice(&tick.seq.to_be_bytes());
    Ok(())
}

fn read_tick(payload: &[u8]) -> Result<Tick, BadFrame> {
    let bytes: [u8; 4] = payload.try_into().map_err(|_| BadFrame)?;
    Ok(Tick {
        seq: u32::from_be_bytes(bytes),
    })
}

/// The same attribute over a writer that cannot fail: a format whose encoder returns nothing is
/// not made to pretend it might.
#[derive(Debug, Outgoing, Serialized)]
#[outgoing(name = "wire.beats")]
#[wire(encode = write_beat)]
struct Beat {
    at: u8,
}

fn write_beat(beat: &Beat, buf: &mut BytesMut) {
    buf.extend_from_slice(&[beat.at]);
}

#[subscriber("wire.ticks")]
async fn count(tick: &Tick) -> HandlerOutcome {
    if tick.seq == 9 {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::drop()
    }
}

#[test]
fn a_value_that_holds_its_bytes_lends_them() {
    let export = Export(vec![1, 2, 3]);
    let mut buf = BytesMut::new();
    // The address alone, so the borrow of `buf` ends with the statement and the buffer can be
    // read back below.
    let lent = export.wire_bytes(&mut buf).expect("held bytes cannot fail");
    let lent = lent.as_ptr();

    // The property the lane exists for: what leaves is the value's own allocation rather than a
    // copy of it, and the publish path's buffer is never written.
    assert!(std::ptr::eq(lent, export.0.as_ptr()));
    assert!(buf.is_empty());
}

#[test]
fn a_value_that_holds_fields_writes_them_into_the_buffer() {
    let tick = Tick { seq: 9 };
    let mut buf = BytesMut::new();

    assert_eq!(tick.wire_bytes(&mut buf).expect("in range"), &[0, 0, 0, 9]);
}

#[test]
fn an_infallible_writer_needs_no_result() {
    let beat = Beat { at: 4 };
    let mut buf = BytesMut::new();

    assert_eq!(beat.wire_bytes(&mut buf).expect("cannot fail"), &[4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_self_serialising_type_makes_the_round_trip() {
    let app =
        RustStream::new(AppInfo::new("wire", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(count);
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Tick { seq: 9 })
        .publish()
        .await
        .expect("inject");

    tb.broker::<MemoryBroker>()
        .subscriber("wire.ticks")
        .assert_called_once()
        .with_raw(&[0, 0, 0, 9])
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("graceful shutdown");
}

// --8<-- [start:assertions]
/// Asserting on a value that serializes itself. The harness's typed assertions decode with a
/// codec, and this lane has none, so both ends of the test speak the type's own format: the
/// expected payload is the frame the writer produces, and a delivery is read back with the
/// reader the type already declares.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_self_serialising_message_is_asserted_on_its_own_bytes() {
    let app =
        RustStream::new(AppInfo::new("wire", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(count);
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Tick { seq: 9 })
        .publish()
        .await
        .expect("inject");

    let broker = tb.broker::<MemoryBroker>();
    let ticks = broker.subscriber("wire.ticks");
    // `from_payload` is the same reader the lane ran on the way in, so the test asserts on the
    // model type without a codec and without repeating the frame layout.
    let received = ticks.received_raw();
    let decoded = Tick::from_payload(&received[0]).expect("a tick frame");
    assert_eq!(decoded, Tick { seq: 9 });

    // The bytes themselves, where the wire format is what the test is pinning.
    ticks
        .assert_called_once()
        .with_raw(&[0, 0, 0, 9])
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("graceful shutdown");
}
// --8<-- [end:assertions]

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_frame_is_settled_by_the_decode_policy() {
    let app =
        RustStream::new(AppInfo::new("wire", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(count);
        });
    let tb = TestApp::start(app).await.expect("harness start");

    // Bytes the reader rejects, injected as bytes so nothing encodes them on the way in.
    tb.message(&Export(vec![1]))
        .to("wire.ticks")
        .publish()
        .await
        .expect("inject");

    tb.broker::<MemoryBroker>()
        .subscriber("wire.ticks")
        .assert_called_once()
        .settled(HandlerOutcome::drop());

    tb.shutdown().await.expect("graceful shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_value_reports_its_own_encoder() {
    let app =
        RustStream::new(AppInfo::new("wire", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(count);
        });
    let tb = TestApp::start(app).await.expect("harness start");

    let err = tb
        .message(&Tick {
            seq: MAX_SEQUENCE + 1,
        })
        .publish()
        .await
        .expect_err("the writer rejects this sequence");

    // The failure names the value's own encoder, not a codec: there is none on this path.
    let reported = err.to_string();
    assert!(
        reported.contains("serializing the message's own bytes failed"),
        "unexpected error: {reported}"
    );
    assert!(
        reported.contains("does not fit the frame"),
        "the format's own error is lost: {reported}"
    );

    tb.shutdown().await.expect("graceful shutdown");
}
