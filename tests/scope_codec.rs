//! Integration coverage for the `include` family on `BrokerScope`, in both codec forms.
//!
//! `with_broker_codec` sets a scope default codec, switching every `include*` call to the
//! `BrokerScope<B, L, C: Codec>` impl block; the bare `with_broker` path uses the default-codec
//! block (`C = ()`). This drives every explicit-source `_on` form (plus batch and
//! batch-publishing) through one codec scope and one default-codec scope, end to end.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use common::{Order, Receipt};
use ruststream::codec::JsonCodec;
use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;

#[subscriber("sc-plain-on")]
async fn plain_on(_o: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("sc-batch")]
async fn batch(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

#[subscriber("sc-batch-on")]
async fn batch_on(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

#[subscriber("sc-pin", publish("sc-pout"))]
async fn relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("sc-pin-on", publish("sc-pout-on"))]
async fn relay_on(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("sc-bpin", publish("sc-bpout"))]
async fn batch_relay(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber("sc-bpin-on", publish("sc-bpout-on"))]
async fn batch_relay_on(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber("sc-pout")]
async fn pout_check(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("sc-pout-on")]
async fn pout_on_check(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("sc-bpout")]
async fn bpout_check(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("sc-bpout-on")]
async fn bpout_on_check(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// Publishes one order to each ingress topic and asserts every named subscription was called
/// exactly once with it - the ingress ones directly, the reply ones through the relay.
async fn drive<S: Send + Sync + 'static>(tb: &TestApp<S>, ingress: &[&str], settled: &[&str]) {
    for topic in ingress {
        tb.message(&Order { id: 1 })
            .to(*topic)
            .publish()
            .await
            .expect("publish");
    }
    for name in settled {
        tb.broker::<MemoryBroker>()
            .subscriber(name)
            .assert_called_once()
            .settled(HandlerOutcome::ack());
    }
}

/// One codec scope, every scope-codec variant mounted: the plain and batch `include`s, and both
/// reply-publishing shapes, each registered twice so the scope codec is proven on every path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scope_codec_include_family_dispatches() {
    let app = RustStream::new(AppInfo::new("sc", "0.1.0")).with_broker_codec(
        MemoryBroker::new(),
        JsonCodec,
        |b| {
            b.include(plain_on);
            b.include(batch.batch(nonzero!(64)));
            b.include(batch_on.batch(nonzero!(64)));
            b.include(relay).out(Reply, Publish);
            b.include(relay_on).out(Reply, Publish);
            b.include(batch_relay.batch(nonzero!(64)))
                .out(Reply, Publish);
            b.include(batch_relay_on.batch(nonzero!(64)))
                .out(Reply, Publish);
            b.include(pout_check);
            b.include(pout_on_check);
            b.include(bpout_check);
            b.include(bpout_on_check);
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    drive(
        &tb,
        &[
            "sc-plain-on",
            "sc-batch",
            "sc-batch-on",
            "sc-pin",
            "sc-pin-on",
            "sc-bpin",
            "sc-bpin-on",
        ],
        &[
            "sc-plain-on",
            "sc-batch",
            "sc-batch-on",
            "sc-pout",
            "sc-pout-on",
            "sc-bpout",
            "sc-bpout-on",
        ],
    )
    .await;
}

#[subscriber("d-plain-on")]
async fn d_plain_on(_o: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("d-batch-on")]
async fn d_batch_on(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

#[subscriber("d-pin-on", publish("d-pout-on"))]
async fn d_relay_on(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

#[subscriber("d-bpin-on", publish("d-bpout-on"))]
async fn d_batch_relay_on(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

#[subscriber("d-pout-on")]
async fn d_pout_on_check(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("d-bpout-on")]
async fn d_bpout_on_check(_r: &Receipt) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// The same family on the default-codec block: the plain and batch `include`s next to both
/// reply-publishing shapes, decoding with the default codec rather than a named one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_codec_include_family_dispatches() {
    let app = RustStream::new(AppInfo::new("dsc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(d_plain_on);
        b.include(d_batch_on.batch(nonzero!(64)));
        b.include(d_relay_on).out(Reply, Publish);
        b.include(d_batch_relay_on.batch(nonzero!(64)))
            .out(Reply, Publish);
        b.include(d_pout_on_check);
        b.include(d_bpout_on_check);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    drive(
        &tb,
        &["d-plain-on", "d-batch-on", "d-pin-on", "d-bpin-on"],
        &["d-plain-on", "d-batch-on", "d-pout-on", "d-bpout-on"],
    )
    .await;
}
