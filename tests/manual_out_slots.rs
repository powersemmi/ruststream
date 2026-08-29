//! The macro-free counterpart of `tests/out_slots.rs`: marker-identified `Out` slots written out
//! as definitions - multi-slot binding by marker, the harness's per-slot capture, and the
//! broker-defined capability extension through [`SlotPublisher::inner`].
//!
//! Slot markers, the slot list and the publisher-generic definition are all ordinary trait impls,
//! so a handler with several injected publishers is reachable with the attribute off.
#![cfg(all(feature = "memory", feature = "json", feature = "testing"))]

use std::marker::PhantomData;

use ruststream::codec::Codec;
use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryPublish, MemoryPublisher};
use ruststream::prelude::*;
use ruststream::runtime::{
    AllOpen, BindSlots, Declared, Decoded, DefaultSlot, HasSlots, InjectCall, InjectDef,
    OutgoingMessageMetadata, PublishExt, RawBytes, Settle, SlotPublisher, SubscriberBuilder, forms,
};
use ruststream::testing::TestApp;
use ruststream::{
    CallerName, ConnectedBroker, MessageHeaders, NoHeaders, OutgoingDestination, PairError,
};
use serde::{Deserialize, Serialize};

/// The whole content of a definition generic over its slot publishers: the inferred types and
/// nothing else, so the definition stays a zero-sized value the mount site builds for free.
type SlotTypes<T> = PhantomData<fn() -> T>;

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

/// Two slots in one handler; no broker publisher type appears anywhere in the definition.
struct Transcode;

impl Declared for Transcode {
    type Form = forms::Out;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("slots.in"))
    }
}

impl HasSlots for Transcode {
    type Markers = (Encoded, Audit);
}

impl<Conn, Enc, EncodedPolicy, AuditPolicy>
    BindSlots<Conn, ((EncodedPolicy, Enc), (AuditPolicy, Enc))> for Transcode
where
    Conn: ConnectedBroker,
    EncodedPolicy: PublishPolicy<Conn>,
    AuditPolicy: PublishPolicy<Conn>,
{
    type Bound = TranscodeDef<
        SlotPublisher<EncodedPolicy::Live, Encoded>,
        SlotPublisher<AuditPolicy::Live, Audit>,
        Enc,
    >;
    type Extra = ((EncodedPolicy, Enc), (AuditPolicy, Enc));

    fn bind(
        self,
        sources: ((EncodedPolicy, Enc), (AuditPolicy, Enc)),
    ) -> (Self::Bound, Self::Extra) {
        (TranscodeDef(PhantomData), sources)
    }
}

struct TranscodeDef<EncodedPub, AuditPub, Enc>(SlotTypes<(EncodedPub, AuditPub, Enc)>);

impl<EncodedPub, AuditPub, Enc> InjectDef for TranscodeDef<EncodedPub, AuditPub, Enc>
where
    EncodedPub: Publisher + Send + Sync + 'static,
    AuditPub: Publisher + Send + Sync + 'static,
    Enc: Codec + Send + Sync + 'static,
{
    type Input = RawBytes;
    type Context = ();
    type Source = Name;
    type Injections = (
        Out<EncodedPub, Encoded, (), Enc>,
        Out<AuditPub, Audit, (), Enc>,
    );

    fn source(&self) -> Self::Source {
        Name::new("slots.in")
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        let mut declared = <Encoded as OutSlot>::outgoing();
        declared.extend(<Audit as OutSlot>::outgoing());
        declared
    }
}

impl<State, EncodedPub, AuditPub, Enc> InjectCall<State> for TranscodeDef<EncodedPub, AuditPub, Enc>
where
    State: Send + Sync,
    EncodedPub: Publisher + Send + Sync + 'static,
    AuditPub: Publisher + Send + Sync + 'static,
    Enc: Codec + Send + Sync + 'static,
{
    async fn call(
        &self,
        chunk: &[u8],
        injections: &Self::Injections,
        _ctx: &mut Context<'_, (), State>,
    ) -> Settle {
        let Out(encoded) = &injections.0;
        let Out(audit) = &injections.1;
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
            b.include(Transcode)
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

/// The definition bounds its slot with the broker-defined capability, not a core one: the
/// publisher generic carries `ShardLanes` where a plain slot would carry `Publisher`.
struct RouteShard;

impl Declared for RouteShard {
    type Form = forms::Out;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("slots.sharded"))
    }
}

impl HasSlots for RouteShard {
    type Markers = (DefaultSlot,);
}

impl<Conn, Enc, Policy> BindSlots<Conn, ((Policy, Enc),)> for RouteShard
where
    Conn: ConnectedBroker,
    Policy: PublishPolicy<Conn>,
{
    type Bound = RouteShardDef<SlotPublisher<Policy::Live, DefaultSlot>, Enc>;
    type Extra = ((Policy, Enc),);

    fn bind(self, sources: ((Policy, Enc),)) -> (Self::Bound, Self::Extra) {
        (RouteShardDef(PhantomData), sources)
    }
}

struct RouteShardDef<Lanes, Enc>(SlotTypes<(Lanes, Enc)>);

impl<Lanes, Enc> InjectDef for RouteShardDef<Lanes, Enc>
where
    Lanes: ShardLanes + Send + Sync + 'static,
    Enc: Codec + Send + Sync + 'static,
{
    type Input = Decoded<Event>;
    type Context = ();
    type Source = Name;
    type Injections = (Out<Lanes, DefaultSlot, (), Enc>,);

    fn source(&self) -> Self::Source {
        Name::new("slots.sharded")
    }
}

impl<State, Lanes, Enc> InjectCall<State> for RouteShardDef<Lanes, Enc>
where
    State: Send + Sync,
    Lanes: ShardLanes + Send + Sync + 'static,
    Enc: Codec + Send + Sync + 'static,
{
    async fn call(
        &self,
        event: &Event,
        injections: &Self::Injections,
        _ctx: &mut Context<'_, (), State>,
    ) -> Settle {
        let Out(lanes) = &injections.0;
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
            b.include(RouteShard).publisher(LanePolicy);
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
