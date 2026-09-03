//! The macro-free counterpart of `tests/out_slots.rs`: marker-identified `Out` slots written out
//! as bodies - multi-slot binding by marker, the harness's per-slot capture, and the
//! broker-defined capability extension through the arena's transparent entry.
//!
//! Slot markers and the entry-generic body are all ordinary trait impls, so a handler with
//! several injected publishers is reachable with the attribute off. The include site binds what
//! the body's arena declares: the marker list comes from the impl, and the input kind follows the
//! body's own parameter, so the raw-input one needs nothing else.
#![cfg(all(feature = "memory", feature = "json", feature = "testing"))]

use std::convert::Infallible;

use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryPublish, MemoryPublisher};
use ruststream::prelude::*;
use ruststream::runtime::{Input, MessageWire, PublishedThrough, SerializedWire, SoloDeserialized};
use ruststream::testing::TestApp;
use ruststream::{
    CallerName, FixedName, MessageHeaders, NoHeaders, OutgoingDestination, PairError,
};
use serde::{Deserialize, Serialize};

// `#[derive(Deserialized)]` by hand: the payload view the slot-publishing body takes, and the
// input spelling that routes it onto the codec-free lane.
struct Frame<'a>(&'a [u8]);

impl Deserialized for Frame<'_> {
    type Output<'a> = Frame<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Frame<'_>, Self::Error> {
        Ok(Frame(payload))
    }
}

impl Input for Frame<'_> {
    type Axis = SoloDeserialized<Frame<'static>>;
}

/// The message the slot publishes carry; it declares no name, so each call site names one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
struct Event {
    id: u64,
}

impl OutgoingDestination for Event {
    type Form = CallerName;
}

impl MessageHeaders for Event {
    type Contract = NoHeaders;
}

/// Bytes published as themselves: what the transcoding body sends when the payload is a frame
/// rather than a model. Written out the way `#[derive(Outgoing, Serialized)]` would write it,
/// and declaring no name, so each call site keeps naming one.
struct Wire(Vec<u8>);

impl Wire {
    fn of(bytes: impl AsRef<[u8]>) -> Self {
        Self(bytes.as_ref().to_vec())
    }
}

impl Serialized for Wire {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl MessageWire for Wire {
    type Wire = SerializedWire;
}

impl OutgoingDestination for Wire {
    type Form = CallerName;
}

impl MessageHeaders for Wire {
    type Contract = NoHeaders;
}

// `#[derive(OutSlot)]` by hand: a unit struct plus the marker's name, which is what the startup
// diagnostics and the harness's per-slot assertions address the slot by.
struct Encoded;

impl OutSlot for Encoded {
    const NAME: &'static str = "Encoded";
}

struct Audit;

impl OutSlot for Audit {
    const NAME: &'static str = "Audit";
}

// The `#[publishes(Wire)]` dictionary of both markers, by hand: the frame and its receipt are
// all that leaves either slot.
impl PublishedThrough<Encoded> for Wire {}

impl PublishedThrough<Audit> for Wire {}

/// Two slots in one handler; no broker publisher type appears anywhere in the body.
struct Transcode;

type TranscodeSlots<EncodedPub, AuditPub, EncA, EncB> =
    Outs<(Slot<Encoded, EncodedPub, EncA>, Slot<Audit, AuditPub, EncB>)>;

impl<'p, EncodedPub, AuditPub, EncA, EncB>
    Handle<Frame<'p>, (), TranscodeSlots<EncodedPub, AuditPub, EncA, EncB>> for Transcode
where
    Slot<Encoded, EncodedPub, EncA>: Publish,
    Slot<Audit, AuditPub, EncB>: Publish,
{
    async fn handle(
        &self,
        chunk: &Frame<'p>,
        outs: &TranscodeSlots<EncodedPub, AuditPub, EncA, EncB>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let mut headers = HeaderMap::new();
        headers.insert("source", "slots.in");
        if outs
            .get(Encoded)
            .message(&Wire::of(chunk.0))
            .with_headers(headers)
            .to("slots.encoded")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        let receipt = chunk.0.len().to_be_bytes();
        if outs
            .get(Audit)
            .message(&Wire::of(receipt))
            .to("slots.audit")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// The slots bind by marker, in either order, and the harness captures per slot (with headers).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slots_bind_by_marker_and_capture_per_slot() {
    // Deliberately bound in the opposite of the marker-list order.
    let app =
        RustStream::new(AppInfo::new("slots", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("slots.in", Transcode).build())
                .out(Audit, MemoryPublish)
                .out(Encoded, MemoryPublish)
                .build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    // --8<-- [start:slot_capture]
    tb.broker::<MemoryBroker>()
        .message(&Wire::of(b"frame"))
        .to("slots.in")
        .publish()
        .await
        .expect("publish");

    let encoded = tb.out::<Encoded>().assert_called_once().with_raw(b"frame");
    let recorded = &encoded.messages()[0];
    assert_eq!(recorded.name(), "slots.encoded");
    assert_eq!(
        recorded.headers().get("source"),
        Some(b"slots.in".as_slice()),
    );
    tb.out::<Audit>()
        .assert_called_once()
        .with_raw(5u64.to_be_bytes().as_slice());
    // --8<-- [end:slot_capture]

    // The broker's publish log sees the same messages; the slot view only adds attribution.
    tb.broker::<MemoryBroker>()
        .published::<Event>("slots.encoded")
        .assert_called_once();
}

// --8<-- [start:serialized_out]
/// A self-carrying model in the slot dictionary, written out: the bytes, the wire spelling
/// that routes a typed publish onto the serialized wire, the declared destination and headers,
/// and the membership - what `#[derive(Serialized)]`, `#[derive(Outgoing)]` and
/// `#[publishes(..)]` would write.
struct WireExport(Vec<u8>);

impl Serialized for WireExport {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl MessageWire for WireExport {
    type Wire = SerializedWire;
}

impl OutgoingDestination for WireExport {
    type Form = FixedName;
    const ADDRESS: &'static str = "slots.exports";
}

impl MessageHeaders for WireExport {
    type Contract = NoHeaders;
}

struct Exports;

impl OutSlot for Exports {
    const NAME: &'static str = "Exports";
}

impl PublishedThrough<Exports> for WireExport {}

/// One typed entry serves both wires: the type picks the lane, so `message(&wire)` publishes
/// the bytes as they are - no codec anywhere - to the destination the declaration names.
struct ExportChunks;

impl<'p, Wired, Enc> Handle<Frame<'p>, (), Outs<(Slot<Exports, Wired, Enc>,)>> for ExportChunks
where
    Slot<Exports, Wired, Enc>: Publish,
{
    async fn handle(
        &self,
        frame: &Frame<'p>,
        outs: &Outs<(Slot<Exports, Wired, Enc>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let wire = WireExport(frame.0.to_vec());
        if outs.get(Exports).message(&wire).publish().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:serialized_out]

/// The serialized dictionary member leaves byte-for-byte through the slot's typed entry, at
/// the destination its declaration names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_serialized_member_publishes_through_the_typed_entry() {
    let app = RustStream::new(AppInfo::new("slots-wire", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(subscriber("slots.chunks", ExportChunks).build())
                .out(Exports, MemoryPublish)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker::<MemoryBroker>()
        .message(&Wire::of(b"chunk"))
        .to("slots.chunks")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");

    tb.out::<Exports>().assert_called_once().with_raw(b"chunk");
    tb.broker::<MemoryBroker>()
        .published::<WireExport>("slots.exports")
        .assert_called_once()
        .with_raw(b"chunk");
}

// --8<-- [start:extension]
// A paired value that is NOT a publisher: a lane router in the shape of a broker's
// per-partition producer cache. The capability is broker-defined; the core knows nothing
// about it.
#[derive(Clone)]
struct LaneRouter {
    publisher: MemoryPublisher,
}

/// The broker-defined capability: pick a destination lane for a shard.
trait ShardLanes {
    fn lane(&self, shard: u64) -> (&MemoryPublisher, &'static str);
}

impl ShardLanes for LaneRouter {
    fn lane(&self, shard: u64) -> (&MemoryPublisher, &'static str) {
        let dest = if shard.is_multiple_of(2) {
            "slots.lane.even"
        } else {
            "slots.lane.odd"
        };
        (&self.publisher, dest)
    }
}

/// The policy half: pure declaration pairing into the router, like a broker's
/// `per_partition()` policy pairs into its producer cache. No `Clone`: resolution consumes it.
struct LanePolicy;

impl PublishPolicy<ConnectedMemoryBroker> for LanePolicy {
    type Live = LaneRouter;

    async fn pair(self, connected: &ConnectedMemoryBroker) -> Result<Self::Live, PairError> {
        Ok(LaneRouter {
            publisher: MemoryPublish.pair(connected).await?,
        })
    }
}

/// The body names the wired live type directly - the arena entry is a transparent window onto
/// it, so the broker-defined capability is called with no grafting machinery in between.
struct RouteShard;

struct Lanes;

impl OutSlot for Lanes {
    const NAME: &'static str = "Lanes";
}

impl<Enc> Handle<Event, (), Outs<(Slot<Lanes, LaneRouter, Enc>,)>> for RouteShard
where
    Enc: Send + Sync,
{
    async fn handle(
        &self,
        event: &Event,
        outs: &Outs<(Slot<Lanes, LaneRouter, Enc>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let (publisher, dest) = outs.get(Lanes).lane(event.id);
        if publisher.message(event).to(dest).publish().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:extension]

/// A broker-defined capability rides the slot machinery end to end; its publishes bypass the
/// slot attribution (they leave through the unwrapped live value) and land in the broker's
/// publish log, the documented boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_defined_capability_extends_the_slot_vocabulary() {
    let app = RustStream::new(AppInfo::new("slots-lanes", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(subscriber("slots.sharded", RouteShard).build())
                .out(Lanes, LanePolicy)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    for id in [2u64, 3u64] {
        tb.message(&Event { id })
            .to("slots.sharded")
            .publish()
            .await
            .expect("publish");
    }

    tb.broker::<MemoryBroker>()
        .published::<Event>("slots.lane.even")
        .assert_called_once()
        .with(&Event { id: 2 });
    tb.broker::<MemoryBroker>()
        .published::<Event>("slots.lane.odd")
        .assert_called_once()
        .with(&Event { id: 3 });
    // The live value's publishes are not attributed to the slot: the capture boundary.
    tb.out::<Lanes>().assert_not_called();
}
