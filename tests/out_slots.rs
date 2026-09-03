//! Marker-identified Out slots: multi-slot binding by marker, capability-refined bounds, the
//! harness's per-slot capture, and the broker-defined capability extension through
//! [`SlotPublisher::inner`].
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

mod common;

use common::{Event, Wire};

use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryPublish, MemoryPublisher};
use ruststream::runtime::{
    AppInfo, DefaultSlot, HandlerOutcome, Out, OwnedTransactionalPublish, PublishExt, RustStream,
    SlotPublisher,
};
use ruststream::testing::TestApp;
use ruststream::{
    Broker, Deserialized, HeaderMap, OutSlot, PairError, PublishPolicy, Publisher, subscriber,
};

/// The payload view the slot-publishing body takes: the delivery's bytes, borrowed.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

// Both slots carry a payload that is not a model of its own (a transcoded frame, a length
// receipt), so the wire type is the whole dictionary each of them publishes.
#[derive(OutSlot)]
#[publishes(Wire)]
struct Encoded;

#[derive(OutSlot)]
#[publishes(Wire)]
struct Audit;

/// Two slots in one handler; no broker publisher type appears anywhere in the signature.
#[subscriber("slots.in")]
async fn transcode(
    chunk: &Frame<'_>,
    Out(encoded): Out<impl Publisher, Encoded>,
    Out(audit): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    headers.insert("source", "slots.in");
    if encoded
        .message(&Wire::of(chunk.0))
        .with_headers(headers)
        .to("slots.encoded")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    let receipt = chunk.0.len().to_be_bytes();
    if audit
        .message(&Wire::of(receipt))
        .to("slots.audit")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The slots bind by marker, in either order, and the harness captures per slot (with headers).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slots_bind_by_marker_and_capture_per_slot() {
    // Deliberately bound in the opposite of the signature order.
    let app =
        RustStream::new(AppInfo::new("slots", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(transcode)
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

/// The second slot targets a different broker through a bound token; the marker still picks the
/// position, so the calls stay order-independent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_binds_a_foreign_broker_through_a_token() {
    let ingress = MemoryBroker::new();
    let other = MemoryBroker::new().bindable();
    let to_other = other.bind(MemoryPublish);

    let app = RustStream::new(AppInfo::new("slots-cross", "0.1.0"))
        .with_broker_labeled("other", other, |_b| {})
        .with_broker_labeled("ingress", ingress, |b| {
            b.include(transcode)
                .out(Encoded, MemoryPublish)
                .out(Audit, to_other)
                .build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.broker_named("ingress")
        .message(&Wire::of(b"xy"))
        .to("slots.in")
        .publish()
        .await
        .expect("publish");

    // Both slots captured regardless of which broker each policy pairs against.
    tb.out::<Encoded>().assert_called_once().with_raw(b"xy");
    tb.out::<Audit>()
        .assert_called_once()
        .with_raw(2u64.to_be_bytes().as_slice());
}

/// A capability-refined slot: the handler settles a ledger through an owned transaction without
/// naming a broker type; the memory publisher provides the capability, and the transaction's
/// typed entry admits what the slot's dictionary admits.
#[subscriber("slots.ledger")]
async fn settle(
    event: &Event,
    Out(tx): Out<impl OwnedTransactionalPublish, Encoded>,
) -> HandlerOutcome {
    let Ok(mut txn) = tx.transaction().await else {
        return HandlerOutcome::retry();
    };
    let payload = serde_json::to_vec(event).expect("serializable");
    if txn
        .message(&Wire::of(payload))
        .to("slots.settled")
        .publish()
        .await
        .is_err()
        || txn.commit().await.is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_capability_refined_slot_pairs_a_transactional_publisher() {
    let app =
        RustStream::new(AppInfo::new("slots-txn", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(settle).out(Encoded, MemoryPublish).build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Event { id: 4 })
        .to("slots.ledger")
        .publish()
        .await
        .expect("publish");

    // The transaction's buffer settles outside the slot scope, so the capture lives in the
    // broker's publish log, not the slot view (the documented attribution boundary).
    tb.broker::<MemoryBroker>()
        .published::<Event>("slots.settled")
        .assert_called_once()
        .with(&Event { id: 4 });
}

// A slot left unbound is a compile error, not a runtime one: `.build()` does not exist until
// every slot is bound, and the error names the missing slot (`MissingSlot<Audit>`). Covered by
// the trybuild case `tests/ui/subscriber_out_unbound_slot.rs`.

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

/// The handler bounds its slot with the broker-defined capability, not a core one.
#[subscriber("slots.sharded")]
async fn route_shard(event: &Event, Out(lanes): Out<impl ShardLanes>) -> HandlerOutcome {
    let (publisher, dest) = lanes.lane(event.id);
    if publisher.message(event).to(dest).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
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
            b.include(route_shard).publisher(LanePolicy);
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
