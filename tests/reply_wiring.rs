//! The reply wiring a mount site's chain builds, driven end to end on the in-memory broker: the
//! codec the chain names encodes the reply, the transform it composes stamps it, the publisher's
//! own base headers reach what leaves through it, and `.transactional()` puts a batch's replies
//! in one broker transaction.
#![cfg(all(
    feature = "testing",
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "cbor"
))]

use std::future::{Future, ready};

use ruststream::codec::{CborCodec, Codec};
use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryPublisher};
use ruststream::runtime::{Outgoing, PublishContext, PublishTransform, for_batch};
use ruststream::testing::TestApp;
use ruststream::{HeaderMap, OutgoingMessage, PairError, PublishPolicy};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
struct Order {
    id: u64,
}

/// The reply, which the slot case below also publishes by hand: it declares no destination of its
/// own, so the mount site's `publish("..")` names one and the slot's builder names the other.
#[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
struct Receipt {
    id: u64,
}

/// Stamps a provenance header, so a reply that went through the chain's transform is
/// distinguishable from one that did not.
struct Stamp;

impl<C> PublishTransform<C> for Stamp {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {
        out.headers_mut().insert("x-stamped", b"1".to_vec());
    }
}

#[subscriber("codec.in", publish("codec.out"))]
async fn encode_reply(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// `.codec(..)` names the reply codec: the reply leaves in CBOR while the request still arrives
/// under the scope's default (JSON).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_named_codec_encodes_the_reply() {
    let app = RustStream::new(AppInfo::new("reply-wiring", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(encode_reply).out(Reply, Publish).codec(CborCodec);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 7 })
        .to("codec.in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("codec.in")
        .assert_called_once()
        .with(&Order { id: 7 })
        .settled(HandlerOutcome::ack());

    let published = tb
        .broker::<MemoryBroker>()
        .published::<Receipt>("codec.out");
    let published = published.assert_called_once();
    let payload = published.messages()[0].payload();
    let decoded: Receipt = CborCodec
        .decode(payload)
        .expect("the reply must decode with the codec the chain named");
    assert_eq!(decoded, Receipt { id: 7 });
    assert!(
        serde_json::from_slice::<Receipt>(payload).is_err(),
        "the default codec must not have encoded this reply",
    );
}

#[subscriber("transform.in", publish("transform.out"))]
async fn stamped_reply(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// `.transform(..)` composes a static publish transform onto the reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chained_transform_stamps_the_reply() {
    let app = RustStream::new(AppInfo::new("reply-wiring", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(stamped_reply)
                .out(Reply, Publish)
                .transform(Stamp);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 3 })
        .to("transform.in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("transform.in")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    let published = tb
        .broker::<MemoryBroker>()
        .published::<Receipt>("transform.out");
    let published = published.assert_called_once().with(&Receipt { id: 3 });
    assert_eq!(
        published.messages()[0].headers().get("x-stamped"),
        Some(b"1".as_slice()),
        "the reply must carry the header the chained transform stamps",
    );
}

/// The header a broker publisher carrying a delivery option for a run of messages contributes to
/// everything that leaves through it: a lane, a partition key, a tenant.
const LANE: &str = "x-lane";

/// A publish policy whose live publisher carries an argument for every message it sends: the
/// shape a broker takes when its option rides the headers rather than the message body.
#[derive(Debug, Clone, Copy)]
struct LanePublish;

impl PublishPolicy<ConnectedMemoryBroker> for LanePublish {
    type Live = Laned;

    fn pair(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<Laned, PairError>> {
        let mut base = HeaderMap::new();
        base.insert(LANE, "west");
        ready(Ok(Laned {
            inner: connected.publisher(),
            base,
        }))
    }
}

/// The live half of [`LanePublish`]: the broker's own publisher plus the base it contributes.
struct Laned {
    inner: MemoryPublisher,
    base: HeaderMap,
}

impl Publisher for Laned {
    type Error = MemoryError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.inner.publish(msg).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        Some(&self.base)
    }
}

#[subscriber("lane.in", publish("lane.out"))]
async fn laned_reply(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// A reply leaves through the publisher the chain named, so the base that publisher contributes
/// is on it - and the chain's own transform still writes over that base.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publishers_base_headers_reach_the_reply() {
    let app = RustStream::new(AppInfo::new("reply-wiring", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(laned_reply)
                .out(Reply, LanePublish)
                .transform(Stamp);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 5 })
        .to("lane.in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("lane.out")
        .assert_called_once()
        .with(&Receipt { id: 5 })
        .with_header(LANE, "west")
        .with_header("x-stamped", b"1");
}

#[subscriber("lane.batch.in", publish("lane.batch.out"))]
async fn laned_batch(orders: &[Order]) -> Vec<Receipt> {
    orders
        .iter()
        .map(|order| Receipt { id: order.id })
        .collect()
}

/// The batch reply path builds its own outgoing message too, so the base has to be on those
/// replies as well.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publishers_base_headers_reach_a_batch_reply() {
    let app = RustStream::new(AppInfo::new("reply-wiring", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(laned_batch.batch(nonzero!(8)))
                .out(Reply, LanePublish);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 6 })
        .to("lane.batch.in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("lane.batch.out")
        .assert_called_once()
        .with(&Receipt { id: 6 })
        .with_header(LANE, "west");
}

#[subscriber("lane.slot.in")]
async fn laned_slot(order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    if out
        .message(&Receipt { id: order.id })
        .to("lane.slot.out")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// A slot publish reaches the same base through the publish builder the body writes, so what
/// leaves a slot carries the bound publisher's argument like a reply does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publishers_base_headers_reach_a_slot_publish() {
    let app = RustStream::new(AppInfo::new("reply-wiring", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(laned_slot).out(DefaultSlot, LanePublish).build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 8 })
        .to("lane.slot.in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("lane.slot.out")
        .assert_called_once()
        .with(&Receipt { id: 8 })
        .with_header(LANE, "west");
}

#[subscriber("batch.in", publish("batch.out"))]
async fn confirm_batch(orders: &[Order]) -> Vec<Receipt> {
    orders
        .iter()
        .map(|order| Receipt { id: order.id })
        .collect()
}

/// `.transactional()` publishes a batch's replies inside one broker transaction, and the
/// batch-only transform still runs on each of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_reply_commits_its_transaction() {
    let app = RustStream::new(AppInfo::new("reply-wiring", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm_batch.batch(nonzero!(8)))
                .out(Reply, TransactionalPublish)
                .batch_transform(for_batch(Stamp))
                .transactional();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 11 })
        .to("batch.in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("batch.in")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    let published = tb
        .broker::<MemoryBroker>()
        .published::<Receipt>("batch.out");
    let published = published.assert_called_once().with(&Receipt { id: 11 });
    assert_eq!(
        published.messages()[0].headers().get("x-stamped"),
        Some(b"1".as_slice()),
        "the batch's replies must carry the batch transform's header",
    );
}
