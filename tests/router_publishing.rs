//! The `Router` publishing include family (single-message, batch and byte-for-byte replies): the
//! chain codec decodes the request while the reply keeps its own, the app's publish layers reach
//! what a router mounts, and the route threads the typed delivery context to the publish path.
#![cfg(all(
    feature = "macros",
    feature = "testing",
    feature = "memory",
    feature = "json",
    feature = "cbor"
))]

mod common;

use std::error::Error;
use std::future::Future;

use common::{Order, Receipt, Wire};
use ruststream::codec::CborCodec;
use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::runtime::{
    ForReply, Outgoing, PublishContext, PublishLayer, PublishNext, PublishPipeline,
    PublishTransform, Reads,
};
use ruststream::testing::TestApp;
use ruststream::{BuildContext, Field};

#[subscriber("rpc-in", publish("rpc-out"))]
async fn rpc_relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

/// The router's chain codec decodes the request; the reply leaves under the reply position's
/// codec, the crate default, so the two formats differ on one registration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chain_codec_decodes_the_request_and_leaves_the_reply_its_own() {
    let router = Router::<MemoryBroker>::new()
        .with_codec(CborCodec)
        .include(rpc_relay)
        .out(Reply, Publish)
        .build();

    let app = RustStream::new(AppInfo::new("rpc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(router);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .with_codec(CborCodec)
        .to("rpc-in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("rpc-out")
        .assert_called_once()
        .with(&Receipt { id: 1 });
}

#[subscriber("bpc-in", publish("bpc-out"))]
async fn bpc_relay(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

/// The same on the batch publishing form: elements decode with the chain codec, replies leave
/// under their own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chain_codec_decodes_a_batch_and_leaves_the_replies_their_own() {
    let router = Router::<MemoryBroker>::new()
        .with_codec(CborCodec)
        .include(bpc_relay.batch(nonzero!(8)))
        .out(Reply, Publish)
        .build();

    let app = RustStream::new(AppInfo::new("bpc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(router);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .with_codec(CborCodec)
        .to("bpc-in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("bpc-out")
        .assert_called_once()
        .with(&Receipt { id: 1 });
}

// A static, app-wide publish middleware that stamps a header onto every reply. Used to prove the
// app's `publish_layer` chain reaches a router-mounted publishing handler.
#[derive(Clone)]
struct StampApp;

impl PublishLayer for StampApp {
    fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> impl Future<Output = Result<(), Box<dyn Error + Send + Sync>>> + Send + 'a {
        out.headers_mut().insert("x-app", b"1".to_vec());
        next.run(out)
    }
}

#[subscriber("rl-in", publish("rl-out"))]
async fn rl_relay(o: &Order) -> Receipt {
    Receipt { id: o.id }
}

/// A router is typed before the app exists, yet its reply pairs at startup, where the app is
/// known, so the app-wide publish layers wrap it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_publish_layer_reaches_router_publishing_handlers() {
    let router = Router::<MemoryBroker>::new()
        .include(rl_relay)
        .out(Reply, Publish)
        .build();

    let app = RustStream::new(AppInfo::new("rl", "0.1.0"))
        .publish_layer(StampApp)
        .with_broker(MemoryBroker::new(), |b| {
            b.include_router(router);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("rl-in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("rl-out")
        .assert_called_once()
        .with(&Receipt { id: 1 })
        .with_header("x-app", b"1");
}

#[subscriber("bl-in", publish("bl-out"))]
async fn bl_relay(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

/// The same on the batch router-publishing path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_publish_layer_reaches_router_batch_publishing_handlers() {
    let router = Router::<MemoryBroker>::new()
        .include(bl_relay.batch(nonzero!(8)))
        .out(Reply, Publish)
        .build();

    let app = RustStream::new(AppInfo::new("bl", "0.1.0"))
        .publish_layer(StampApp)
        .with_broker(MemoryBroker::new(), |b| {
            b.include_router(router);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("bl-in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("bl-out")
        .assert_called_once()
        .with(&Receipt { id: 1 })
        .with_header("x-app", b"1");
}

/// A byte-for-byte reply: its bytes are the payload, so the router's route for it carries a
/// policy and no codec.
#[subscriber("raw-in", publish("raw-out"))]
async fn raw_relay(o: &Order) -> Wire {
    Wire::of(o.id.to_be_bytes())
}

/// The byte-for-byte reply route of a router publishes the reply's own bytes to the mount-site
/// destination.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_publishes_a_byte_for_byte_reply() {
    let router = Router::<MemoryBroker>::new()
        .include(raw_relay)
        .out(Reply, Publish)
        .build();

    let app = RustStream::new(AppInfo::new("raw", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(router);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 4 })
        .to("raw-in")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<()>("raw-out")
        .assert_called_once()
        .with_raw(4u32.to_be_bytes().as_slice());
}

// A typed delivery context on a ROUTER-mounted publishing handler: the route threads
// `D::Context`, so a publish layer can read the delivery by key.
#[derive(Default)]
struct TraceCtx {
    correlation: Option<String>,
}

impl BuildContext<MemoryMessage> for TraceCtx {
    fn build(msg: &MemoryMessage) -> Self {
        Self {
            correlation: msg.headers().correlation_id().map(str::to_owned),
        }
    }
}

#[derive(Clone, Copy)]
struct Correlation;

impl Field<TraceCtx> for Correlation {
    type Value<'a> = Option<&'a str>;
    fn get(self, c: &TraceCtx) -> Option<&str> {
        c.correlation.as_deref()
    }
}

struct PropagateCorrelation;

impl<Options> PublishTransform<ForReply<TraceCtx>, Options> for PropagateCorrelation {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, TraceCtx>,
    ) {
        if let Some(id) = cx.context(Correlation) {
            out.headers_mut()
                .insert("correlation-id", id.as_bytes().to_vec());
        }
    }
}

#[subscriber("tc-in", publish("tc-out"))]
async fn tc_relay(o: &Order, _ctx: &mut Context<'_, TraceCtx>) -> Receipt {
    Receipt { id: o.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_publishing_threads_typed_delivery_context() {
    let router = Router::<MemoryBroker>::new()
        .include(tc_relay)
        .out(Reply, Publish)
        .transform(PropagateCorrelation)
        .build();

    let app = RustStream::new(AppInfo::new("tc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(router);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    let mut headers = HeaderMap::new();
    headers.insert("correlation-id", "trace-xyz");
    tb.message(&Order { id: 1 })
        .with_headers(headers)
        .to("tc-in")
        .publish()
        .await
        .expect("publish");

    // A router publishing handler must thread its typed delivery context to the publish layer,
    // which is what lets the transform copy the correlation id onto the reply.
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("tc-out")
        .assert_called_once()
        .with(&Receipt { id: 1 })
        .with_header("correlation-id", b"trace-xyz");
}
