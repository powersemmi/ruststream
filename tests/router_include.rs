//! Integration tests for the `Router` include family (subscribe and batch forms). Also covers
//! `merge`, the router's own `layer` stack, and `handlers()` metadata collection.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

use crate::common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::{Order, Receipt};
use ruststream::memory::{MemoryBroker, MemoryMessage, MemoryPublish};
use ruststream::runtime::{
    AppInfo, Context, ForReply, Handler, HandlerOutcome, Layer, Outgoing, PublishContext,
    PublishTransform, Reads, Router, RouterDef, RustStream, SubscriberSettings,
    layers::TracingLayer,
};
use ruststream::testing::TestApp;
use ruststream::{nonzero, subscriber};

/// Publishes one order to each topic and asserts every one of them reached its handler exactly
/// once. Nothing else is recorded: the subject is which registrations mount, not what they do.
async fn drive_all<S: Send + Sync + 'static>(tb: &TestApp<S>, topics: &[&str]) {
    for topic in topics {
        tb.message(&Order { id: 1 })
            .to(*topic)
            .publish()
            .await
            .expect("publish");
    }
    for topic in topics {
        tb.broker::<MemoryBroker>()
            .subscriber(topic)
            .assert_called_once()
            .with(&Order { id: 1 })
            .settled(HandlerOutcome::ack());
    }
}

#[subscriber("ri-plain")]
async fn ri_plain(_o: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("ri-batch")]
async fn ri_batch(orders: &[Order]) -> HandlerOutcome {
    let _ = orders;
    HandlerOutcome::ack()
}

/// A router mounts the single-message and the batch form alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_includes_dispatch() {
    let router = Router::<MemoryBroker>::new()
        .include(ri_plain)
        .include(ri_batch.batch(nonzero!(64)));

    let app = RustStream::new(AppInfo::new("ri", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup failed");

    drive_all(&tb, &["ri-plain", "ri-batch"]).await;
}

#[subscriber("rm-a")]
async fn rm_a(_o: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("rm-b")]
async fn rm_b(_o: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

/// `merge` keeps both routers' registrations (and their metadata order: own first, merged
/// after); a router-scope `layer` on the merged router still dispatches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merged_router_dispatches_and_collects_metadata() {
    let merged = Router::<MemoryBroker>::new().include(rm_a).merge(
        Router::<MemoryBroker>::new()
            .layer(TracingLayer::default())
            .include(rm_b),
    );

    let names: Vec<_> = merged.handlers().into_iter().map(|m| m.name).collect();
    assert_eq!(names, ["rm-a", "rm-b"]);

    let app = RustStream::new(AppInfo::new("rm", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(merged));
    let tb = TestApp::start(app).await.expect("startup failed");

    drive_all(&tb, &["rm-a", "rm-b"]).await;
}

/// A layer fixed to one message type - the shape a runtime-composed
/// [`DynStack`](ruststream::runtime::DynStack) has - has no blanket impl, so it rides one
/// registration: `.layer(..)` after an `include` wraps that registration outside its decode step.
/// The counter proves the wrapper ran, and that it saw only the handler it was named on.
#[derive(Clone)]
struct CountRaw(Arc<AtomicUsize>);

struct Counted<H>(Arc<AtomicUsize>, H);

impl<H> Layer<H> for CountRaw {
    type Handler = Counted<H>;

    fn layer(&self, inner: H) -> Counted<H> {
        Counted(Arc::clone(&self.0), inner)
    }
}

// Fixed to the broker's own message type, exactly as a `DynStack<MemoryMessage>` is: this is
// what cannot be written as a `BlanketLayer`.
impl<H: Handler<MemoryMessage>> Handler<MemoryMessage> for Counted<H> {
    async fn handle(&self, msg: &MemoryMessage, ctx: &mut Context<'_>) -> HandlerOutcome {
        self.0.fetch_add(1, Ordering::SeqCst);
        self.1.handle(msg, ctx).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_registration_layer_wraps_only_the_registration_it_follows() {
    let hits = Arc::new(AtomicUsize::new(0));
    let routes = Router::<MemoryBroker>::new()
        .include(rm_a)
        .layer(CountRaw(Arc::clone(&hits)))
        .include(rm_b);

    let app = RustStream::new(AppInfo::new("registration-layer", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(routes));
    let tb = TestApp::start(app).await.expect("startup failed");

    drive_all(&tb, &["rm-a", "rm-b"]).await;

    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "the layer rides the registration named before it, and no other",
    );
}

#[subscriber("rc-in", reply("rc-out"))]
async fn rc_confirm(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

/// A mount with steps is followed directly by the next `include`: the chain commits the
/// registration it was building and mounts the next one, as `.build().include(..)` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mount_chain_goes_on_to_the_next_include() {
    let router = Router::<MemoryBroker>::new()
        .include(rc_confirm)
        .out_reply(MemoryPublish)
        .include(ri_plain);

    let app = RustStream::new(AppInfo::new("chain", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup failed");
    drive_all(&tb, &["ri-plain"]).await;
    tb.message(&Order { id: 7 })
        .to("rc-in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("rc-out")
        .assert_called_once()
        .with(&Receipt { id: 7 });
}

#[subscriber("rg-in", reply("rg-out"))]
async fn rg_confirm(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

/// A reply transform that stamps every reply it sees.
struct StampReply;

impl<C, Options> PublishTransform<ForReply<C>, Options> for StampReply {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        _cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut().insert("x-stamp", b"1".to_vec());
    }
}

/// A router a module builds and returns behind `impl RouterDef`, the way a service splits its
/// routes across files.
fn returned_routes() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    Router::<MemoryBroker>::new().include(ri_plain)
}

/// A router returned from a function grows at the caller: another mount, and a reply transform
/// on it, chain onto the routes the function built.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_returned_router_takes_more_mounts() {
    let router = returned_routes()
        .include(rg_confirm)
        .out_reply(MemoryPublish)
        .transform(StampReply)
        .include(ri_batch.batch(nonzero!(64)));

    let app = RustStream::new(AppInfo::new("grow", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include_router(router));
    let tb = TestApp::start(app).await.expect("startup failed");
    drive_all(&tb, &["ri-plain", "ri-batch"]).await;
    tb.message(&Order { id: 9 })
        .to("rg-in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("rg-out")
        .assert_called_once()
        .with(&Receipt { id: 9 })
        .with_header("x-stamp", b"1");
}
