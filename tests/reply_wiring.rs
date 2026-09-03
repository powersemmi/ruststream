//! The reply wiring a mount site's chain builds, driven end to end on the in-memory broker: the
//! codec the chain names encodes the reply, the transform it composes stamps it, and
//! `.transactional()` puts a page's replies in one broker transaction.
#![cfg(all(
    feature = "testing",
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "cbor"
))]

use ruststream::codec::{CborCodec, Codec};
use ruststream::memory::prelude::*;
use ruststream::runtime::{Outgoing, PublishContext, PublishTransform, for_batch};
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, Serialize, PartialEq)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
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
            b.include(encode_reply).publisher(Publish).codec(CborCodec);
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
            b.include(stamped_reply).publisher(Publish).transform(Stamp);
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

#[subscriber("page.in", publish("page.out"))]
async fn confirm_page(orders: &[Order]) -> Vec<Receipt> {
    orders
        .iter()
        .map(|order| Receipt { id: order.id })
        .collect()
}

/// `.transactional()` publishes a page's replies inside one broker transaction, and the
/// batch-only transform still runs on each of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_reply_commits_its_transaction() {
    let app = RustStream::new(AppInfo::new("reply-wiring", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm_page)
                .publisher(TransactionalPublish)
                .batch_transform(for_batch(Stamp))
                .transactional();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 11 })
        .to("page.in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("page.in")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    let published = tb.broker::<MemoryBroker>().published::<Receipt>("page.out");
    let published = published.assert_called_once().with(&Receipt { id: 11 });
    assert_eq!(
        published.messages()[0].headers().get("x-stamped"),
        Some(b"1".as_slice()),
        "the page's replies must carry the batch transform's header",
    );
}
