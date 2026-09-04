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

use ruststream::PairError;
use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryPublisher};
use ruststream::runtime::{OutTransform, Outgoing};
use ruststream::testing::TestApp;
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

impl<'p, E, A> Handle<Frame<'p>, (), Outs<(E, A)>> for Transcode
where
    E: OutEntry<Encoded, Wire: Publisher>,
    A: OutEntry<Audit, Wire: Publisher>,
{
    async fn handle(
        &self,
        chunk: &Frame<'p>,
        outs: &Outs<(E, A)>,
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
                .out(Audit, Publish)
                .out(Encoded, Publish)
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

impl<'p, E> Handle<Frame<'p>, (), Outs<(E,)>> for ExportChunks
where
    E: OutEntry<Exports, Wire: Publisher>,
{
    async fn handle(
        &self,
        frame: &Frame<'p>,
        outs: &Outs<(E,)>,
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
                .out(Exports, Publish)
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

// Grafted onto the arena entry once, for every marker, delegating through the entry's
// transparent `Deref`: this is how a broker crate extends the slot vocabulary with its own
// traits. A body holds the entry, so without this impl the capability is reachable by autoderef
// for a method call but never satisfies a trait bound.
impl<M, W: ShardLanes, E, Pipe, Body> ShardLanes for Slot<M, W, E, Pipe, Body> {
    fn lane(&self, shard: u64) -> (&MemoryPublisher, &'static str) {
        (**self).lane(shard)
    }
}

/// The bound the graft buys: a helper generic over the capability, not over the concrete live
/// type, takes the entry a body holds.
async fn sent<L: ShardLanes + Sync>(lanes: &L, event: &Event) -> bool {
    let (publisher, dest) = lanes.lane(event.id);
    publisher.message(event).to(dest).publish().await.is_ok()
}

/// The policy half: pure declaration pairing into the router, like a broker's
/// `per_partition()` policy pairs into its producer cache. No `Clone`: resolution consumes it.
struct LanePolicy;

impl PublishPolicy<ConnectedMemoryBroker> for LanePolicy {
    type Live = LaneRouter;

    async fn pair(self, connected: &ConnectedMemoryBroker) -> Result<Self::Live, PairError> {
        Ok(LaneRouter {
            publisher: Publish.pair(connected).await?,
        })
    }
}

/// The body leaves the wired live value generic and bounds it with the broker-defined
/// capability, exactly as the attribute's `Out<impl ShardLanes>` does.
struct RouteShard;

struct Lanes;

impl OutSlot for Lanes {
    const NAME: &'static str = "Lanes";
}

impl<L> Handle<Event, (), Outs<(L,)>> for RouteShard
where
    L: OutEntry<Lanes, Wire: ShardLanes>,
{
    async fn handle(
        &self,
        event: &Event,
        outs: &Outs<(L,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if sent(outs.get(Lanes), event).await {
            Ok(())
        } else {
            Err(HandlerOutcome::retry())
        }
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

/// The slot transform the mount below composes on top of the entry's publish path.
struct Envelope;

impl OutTransform for Envelope {
    fn apply(&self, out: &mut Outgoing<'_>) {
        out.headers_mut().insert("x-outbox", b"1".to_vec());
    }
}

/// What the entry bound buys: this body's `where` clause says nothing about the publish path the
/// mount composes, so the one body below is mounted both bare and under `.transform(Envelope)`.
struct Receipt;

impl<A> Handle<Event, (), Outs<(A,)>> for Receipt
where
    A: OutEntry<Audit, Wire: Publisher>,
{
    async fn handle(
        &self,
        event: &Event,
        outs: &Outs<(A,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if outs
            .get(Audit)
            .message(&Wire::of(event.id.to_be_bytes()))
            .to("slots.receipts")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// Mounts the one `Receipt` body under both publish paths: the bare slot sends what the body
/// built, the transformed slot sends it stamped. Neither mount is visible in the body, which is
/// the point - the pipeline is a projection of the entry, not a parameter of the signature.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_body_mounts_bare_and_under_a_slot_transform() {
    let bare = RustStream::new(AppInfo::new("slots-bare", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(subscriber("slots.receipt", Receipt).build())
                .out(Audit, Publish)
                .build();
        },
    );
    let tb = TestApp::start(bare).await.expect("harness start");
    tb.message(&Event { id: 7 })
        .to("slots.receipt")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");
    let bare_receipt = tb
        .out::<Audit>()
        .assert_called_once()
        .with_raw(7u64.to_be_bytes().as_slice());
    assert_eq!(bare_receipt.messages()[0].headers().get("x-outbox"), None);
    tb.shutdown().await.expect("graceful shutdown");

    let stamped = RustStream::new(AppInfo::new("slots-stamped", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(subscriber("slots.receipt", Receipt).build())
                .out(Audit, Publish)
                .transform(Envelope)
                .build();
        },
    );
    let tb = TestApp::start(stamped).await.expect("harness start");
    tb.message(&Event { id: 7 })
        .to("slots.receipt")
        .publish()
        .await
        .expect("publish");
    tb.settle().await.expect("settle");
    tb.out::<Audit>()
        .assert_called_once()
        .with_raw(7u64.to_be_bytes().as_slice())
        .with_header("x-outbox", b"1");
    tb.shutdown().await.expect("graceful shutdown");
}
