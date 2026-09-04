//! Marker-identified Out slots: multi-slot binding by marker, capability-refined bounds, the
//! harness's per-slot capture, and the broker-defined capability extension grafted onto the
//! arena entry.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

mod common;

use common::{Event, Wire};

use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryPublisher};
use ruststream::testing::TestApp;
use ruststream::{OutgoingMessage, PairError};

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

/// The second slot targets a different broker through a bound token; the marker still picks the
/// position, so the calls stay order-independent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_binds_a_foreign_broker_through_a_token() {
    let ingress = MemoryBroker::new();
    let other = MemoryBroker::new().bindable();
    let to_other = other.bind(Publish);

    let app = RustStream::new(AppInfo::new("slots-cross", "0.1.0"))
        .with_broker_labeled("other", other, |_b| {})
        .with_broker_labeled("ingress", ingress, |b| {
            b.include(transcode)
                .out(Encoded, Publish)
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
async fn settle(event: &Event, Out(tx): Out<impl OwnedTransactions, Encoded>) -> HandlerOutcome {
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
            b.include(settle).out(Encoded, TransactionalPublish).build();
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

/// The same settling, driven through the raw capability methods instead of the typed openers:
/// the entry delegates each of them, so a body that drives `begin_transaction` / `commit` by
/// hand keeps the slot attribution on what it publishes in between.
#[subscriber("slots.by-hand")]
async fn settle_by_hand(
    event: &Event,
    Out(journal): Out<impl TransactionalPublisher, Encoded>,
) -> HandlerOutcome {
    if TransactionalPublisher::begin_transaction(journal)
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    let payload = serde_json::to_vec(event).expect("serializable");
    if journal
        .message(&Wire::of(payload))
        .to("slots.by-hand.settled")
        .publish()
        .await
        .is_err()
        || TransactionalPublisher::commit(journal).await.is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_raw_capability_calls_keep_the_slot_attribution() {
    let app = RustStream::new(AppInfo::new("slots-by-hand", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(settle_by_hand)
                .out(Encoded, TransactionalPublish)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Event { id: 6 })
        .to("slots.by-hand")
        .publish()
        .await
        .expect("publish");

    // The publish went through the entry, so the slot view has it; the commit released it to the
    // broker, so the publish log has it too.
    tb.out::<Encoded>()
        .assert_called_once()
        .decoded_as::<Event>()
        .with(&Event { id: 6 });
    tb.broker::<MemoryBroker>()
        .published::<Event>("slots.by-hand.settled")
        .assert_called_once()
        .with(&Event { id: 6 });
}

/// A broker refusing a raw capability call reports it in the entry's own error type, so the body
/// settles on the refusal instead of unwinding. The body commits with nothing open, the refusal
/// the in-memory broker gives on demand.
#[subscriber("slots.refused")]
async fn commit_without_begin(
    _event: &Event,
    Out(journal): Out<impl TransactionalPublisher, Encoded>,
) -> HandlerOutcome {
    if TransactionalPublisher::commit(journal).await.is_err() {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_capability_call_comes_back_as_the_entrys_error() {
    let app = RustStream::new(AppInfo::new("slots-refused", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(commit_without_begin)
                .out(Encoded, TransactionalPublish)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Event { id: 8 })
        .to("slots.refused")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("slots.refused")
        .assert_called_once()
        .settled(HandlerOutcome::drop());
    tb.out::<Encoded>().assert_not_called();
}

/// The typed opener and the raw one share the name `transaction`, and the inherent typed one
/// wins; the raw capability method stays reachable through the trait path. What it hands back is
/// the broker's own transaction value, so the body publishes into it at the byte level - and
/// what that buffer settles leaves outside the slot, in the broker's publish log alone.
#[subscriber("slots.raw-owned")]
async fn settle_raw(
    event: &Event,
    Out(ledger): Out<impl OwnedTransactions, Encoded>,
) -> HandlerOutcome {
    let Ok(mut txn) = OwnedTransactions::transaction(ledger).await else {
        return HandlerOutcome::retry();
    };
    let payload = serde_json::to_vec(event).expect("serializable");
    if txn
        .publish(OutgoingMessage::new("slots.raw-owned.settled", &payload))
        .await
        .is_err()
        || txn.commit().await.is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_raw_owned_transaction_settles_outside_the_slot() {
    let app = RustStream::new(AppInfo::new("slots-raw-owned", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(settle_raw)
                .out(Encoded, TransactionalPublish)
                .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Event { id: 5 })
        .to("slots.raw-owned")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Event>("slots.raw-owned.settled")
        .assert_called_once()
        .with(&Event { id: 5 });
    tb.out::<Encoded>().assert_not_called();
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

// Grafted onto the arena entry once, for every marker, delegating through the entry's
// transparent `Deref`: this is how a broker crate extends the slot vocabulary with its own
// traits. A handler body holds the entry, so without this impl the capability is reachable by
// autoderef for a method call but never satisfies a trait bound.
impl<M, W: ShardLanes, E, Pipe, Body> ShardLanes for Slot<M, W, E, Pipe, Body> {
    fn lane(&self, shard: u64) -> (&MemoryPublisher, &'static str) {
        (**self).lane(shard)
    }
}

/// The bound the graft buys: a helper generic over the capability, not over the concrete live
/// type, takes the entry a handler body holds.
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

/// The handler bounds its slot with the broker-defined capability, not a core one.
#[subscriber("slots.sharded")]
async fn route_shard(event: &Event, Out(lanes): Out<impl ShardLanes>) -> HandlerOutcome {
    if sent(lanes, event).await {
        HandlerOutcome::ack()
    } else {
        HandlerOutcome::retry()
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
            b.include(route_shard).out(DefaultSlot, LanePolicy).build();
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
