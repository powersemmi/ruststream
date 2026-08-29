//! The macro-free counterpart of `tests/out_slots.rs`: marker-identified `Out` slots written out
//! as bodies - multi-slot binding by marker, the harness's per-slot capture, and the
//! broker-defined capability extension through [`SlotPublisher::inner`].
//!
//! Slot markers and the publisher-generic body are all ordinary trait impls, so a handler with
//! several injected publishers is reachable with the attribute off. `with_slots` binds both
//! bodies below: the input kind follows the body's own parameter, so the raw-input one is
//! `with_slots::<[u8], ..>` and needs nothing else.
#![cfg(all(feature = "memory", feature = "json", feature = "testing"))]

use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryPublish, MemoryPublisher};
use ruststream::prelude::*;
use ruststream::runtime::SlotPublisher;
use ruststream::testing::TestApp;
use ruststream::{CallerName, MessageHeaders, NoHeaders, OutgoingDestination, PairError};
use serde::{Deserialize, Serialize};

/// The message the slot publishes carry; it declares no name, so each call site names one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Event {
    id: u64,
}

impl OutgoingDestination for Event {
    type Form = CallerName;
}

impl MessageHeaders for Event {
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

/// Two slots in one handler; no broker publisher type appears anywhere in the body.
struct Transcode;

type TranscodeSlots<EncodedPub, AuditPub, Enc> = (
    Out<EncodedPub, Encoded, (), Enc>,
    Out<AuditPub, Audit, (), Enc>,
);

impl<State, EncodedPub, AuditPub, Enc>
    SlotsHandler<[u8], TranscodeSlots<EncodedPub, AuditPub, Enc>, (), State> for Transcode
where
    State: Send + Sync,
    EncodedPub: Publisher,
    AuditPub: Publisher,
    Enc: Send + Sync,
{
    async fn handle(
        &self,
        chunk: &[u8],
        slots: &TranscodeSlots<EncodedPub, AuditPub, Enc>,
        _ctx: &mut Context<'_, (), State>,
    ) -> Settle {
        let Out(encoded) = &slots.0;
        let Out(audit) = &slots.1;
        let mut headers = HeaderMap::new();
        headers.insert("source", "slots.in");
        if encoded
            .raw(chunk)
            .with_headers(headers)
            .to("slots.encoded")
            .publish()
            .await
            .is_err()
        {
            return HandlerResult::retry().into();
        }
        let receipt = chunk.len().to_be_bytes();
        if audit
            .raw(&receipt)
            .to("slots.audit")
            .publish()
            .await
            .is_err()
        {
            return HandlerResult::retry().into();
        }
        HandlerResult::Ack.into()
    }
}

/// The slots bind by marker, in either order, and the harness captures per slot (with headers).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slots_bind_by_marker_and_capture_per_slot() {
    // Deliberately bound in the opposite of the marker-list order.
    let app =
        RustStream::new(AppInfo::new("slots", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(with_slots::<[u8], (Encoded, Audit), _, _>(
                "slots.in", Transcode,
            ))
            .out(Audit, MemoryPublish)
            .out(Encoded, MemoryPublish)
            .mount();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    // --8<-- [start:slot_capture]
    tb.broker::<MemoryBroker>()
        .raw(b"frame")
        .to("slots.in")
        .publish()
        .await
        .expect("raw publish");

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

// Grafted onto the slot wrapper once, for every marker: this is what `SlotPublisher::inner`
// exists for, and how a broker crate extends the slot vocabulary with its own traits.
impl<P: ShardLanes, M: OutSlot> ShardLanes for SlotPublisher<P, M> {
    fn lane(&self, shard: u64) -> (&MemoryPublisher, &'static str) {
        self.inner().lane(shard)
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

/// The body bounds its slot with the broker-defined capability, not a core one: the publisher
/// generic carries `ShardLanes` where a plain slot would carry `Publisher`.
struct RouteShard;

impl<Lanes, Enc, State> SlotsHandler<Event, (Out<Lanes, DefaultSlot, (), Enc>,), (), State>
    for RouteShard
where
    Lanes: ShardLanes + Send + Sync,
    Enc: Send + Sync,
    State: Send + Sync,
{
    async fn handle(
        &self,
        event: &Event,
        slots: &(Out<Lanes, DefaultSlot, (), Enc>,),
        _ctx: &mut Context<'_, (), State>,
    ) -> Settle {
        let Out(lanes) = &slots.0;
        let (publisher, dest) = lanes.lane(event.id);
        let payload = serde_json::to_vec(event).expect("serializable");
        if publisher.raw(&payload).to(dest).publish().await.is_err() {
            return HandlerResult::retry().into();
        }
        HandlerResult::Ack.into()
    }
}
// --8<-- [end:extension]

/// A broker-defined capability rides the slot machinery end to end; its publishes bypass the
/// slot attribution (they leave through the unwrapped inner value) and land in the broker's
/// publish log, the documented boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_defined_capability_extends_the_slot_vocabulary() {
    let app = RustStream::new(AppInfo::new("slots-lanes", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(with_slots::<Event, (DefaultSlot,), _, _>(
                "slots.sharded",
                RouteShard,
            ))
            .publisher(LanePolicy);
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
    // The inner value's publishes are not attributed to the slot: the capture boundary.
    tb.out::<DefaultSlot>().assert_not_called();
}
