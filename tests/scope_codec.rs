//! The scope codec on the `include` family: `with_broker_codec` names the codec every
//! registration in the scope decodes with, while a reply encodes with the codec its own mount
//! names, the default one here.
//!
//! The scope names CBOR, not the default JSON: a registration that fell back to the default codec
//! would fail to decode the CBOR payloads injected below.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "cbor",
    feature = "testing"
))]

mod common;

use common::{Order, Receipt};
use ruststream::codec::CborCodec;
use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;

#[subscriber("sc-plain")]
async fn plain(_o: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("sc-batch")]
async fn batch(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

#[subscriber("sc-pin", publish("sc-pout"))]
async fn relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("sc-bpin", publish("sc-bpout"))]
async fn batch_relay(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

/// One codec scope with every registration shape mounted: the plain and batch `include`s, and
/// both reply-publishing shapes. The requests decode with the scope codec; the replies leave in
/// the default codec, since no mount names one for them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scope_codec_include_family_dispatches() {
    let app = RustStream::new(AppInfo::new("sc", "0.1.0")).with_broker_codec(
        MemoryBroker::new(),
        CborCodec,
        |b| {
            b.include(plain);
            b.include(batch.batch(nonzero!(64)));
            b.include(relay).out(Reply, Publish);
            b.include(batch_relay.batch(nonzero!(64)))
                .out(Reply, Publish);
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    for topic in ["sc-plain", "sc-batch", "sc-pin", "sc-bpin"] {
        tb.message(&Order { id: 1 })
            .with_codec(CborCodec)
            .to(topic)
            .publish()
            .await
            .expect("publish");
        tb.broker::<MemoryBroker>()
            .subscriber(topic)
            .assert_called_once()
            .with_codec(&CborCodec, &Order { id: 1 })
            .settled(HandlerOutcome::ack());
    }
    for reply in ["sc-pout", "sc-bpout"] {
        tb.broker::<MemoryBroker>()
            .published::<Receipt>(reply)
            .assert_called_once()
            .with(&Receipt { id: 1 });
    }
}
