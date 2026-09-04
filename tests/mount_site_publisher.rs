//! The reply publisher is named at the mount site, on both surfaces, and the app-wide publish
//! pipeline reaches what each surface can reach.
#![cfg(all(
    feature = "memory",
    feature = "macros",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::error::Error;
use std::future::{Future, ready};

use common::{Order, Wire};

use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemoryError, MemoryPublisher};
use ruststream::runtime::{Outgoing, PublishLayer, PublishNext, PublishPipeline};
use ruststream::testing::TestApp;
use ruststream::{OutgoingMessage, PairError, PublishPolicy};

/// The app-wide middleware: it stamps every publish the service makes.
#[derive(Clone)]
struct AppStamp;

impl PublishLayer for AppStamp {
    async fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        out.headers_mut().insert("x-app", b"1".to_vec());
        next.run(out).await
    }
}

#[derive(OutSlot)]
#[publishes(Order)]
struct Audit;

/// A batch handler that answers: the reply's publisher is the mount site's to name, on a scope
/// and on a router alike.
#[subscriber("mount.orders", publish("mount.receipts"))]
async fn confirm_batch(orders: &[Order]) -> Vec<Order> {
    orders.iter().map(|order| Order { id: order.id }).collect()
}

/// A slot-carrying handler, to see which surface's slots reach the app-wide pipeline.
#[subscriber("mount.mirror")]
async fn mirror(order: &Order, Out(audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    if audit
        .message(order)
        .to("mount.audit")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The batch's reply leaves through the policy the include site named, and the app-wide publish
/// layer stamps it: nothing about the publisher is on the definition.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_reply_takes_the_policy_the_include_site_names() {
    let app = RustStream::new(AppInfo::new("mount-scope", "0.1.0"))
        .publish_layer(AppStamp)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(confirm_batch.batch(nonzero!(4)))
                .out(Reply, Publish);
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 7 })
        .to("mount.orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Order>("mount.receipts")
        .assert_called_once()
        .with(&Order { id: 7 })
        .with_header("x-app", b"1");
}

/// The same registration through a router: the terminal differs, the vocabulary does not, and
/// the reply still travels the app's publish pipeline (the publisher pairs at startup, where the
/// app is known).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_names_the_same_reply_policy() {
    let routes = Router::<MemoryBroker>::new()
        .include(confirm_batch.batch(nonzero!(4)))
        .out(Reply, Publish)
        .build();
    let app = RustStream::new(AppInfo::new("mount-router", "0.1.0"))
        .publish_layer(AppStamp)
        .with_broker(MemoryBroker::new(), |b| {
            b.include_router(routes);
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 9 })
        .to("mount.orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Order>("mount.receipts")
        .assert_called_once()
        .with(&Order { id: 9 })
        .with_header("x-app", b"1");
}

/// A broker's own publish policy, with one option of its own: the shape every broker crate
/// ships next to `Publish`.
#[derive(Clone, Copy, Default)]
struct Prefixed(&'static str);

impl Prefixed {
    /// The setter its settings trait drives; a real broker's takes `self` for the same reason,
    /// so the option can be named on a policy value as well as through the chain.
    #[allow(clippy::unused_self)]
    fn prefix(self, prefix: &'static str) -> Self {
        Self(prefix)
    }
}

impl PublishPolicy<ConnectedMemoryBroker> for Prefixed {
    type Live = PrefixedPublisher;

    fn pair(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(PrefixedPublisher {
            prefix: self.0,
            inner: connected.publisher(),
        }))
    }
}

struct PrefixedPublisher {
    prefix: &'static str,
    inner: MemoryPublisher,
}

impl Publisher for PrefixedPublisher {
    type Error = MemoryError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let name = format!("{}{}", self.prefix, msg.name());
        let forwarded =
            OutgoingMessage::new(name.as_str(), msg.payload()).with_headers(msg.headers().clone());
        self.inner.publish(forwarded).await
    }
}

/// What a broker crate layers on top: its publisher's own settings, bound to its policy so the
/// methods do not exist on a chain that named another broker's - the publish-side mirror of a
/// subscription's `map_source` extension trait.
trait PrefixSettings: Sized {
    fn prefixed(self, prefix: &'static str) -> Self;
}

impl<T: MapPublisher<Policy = Prefixed>> PrefixSettings for T {
    fn prefixed(self, prefix: &'static str) -> Self {
        self.map_publisher(|policy| policy.prefix(prefix))
    }
}

/// One impl covers the reply position on a scope...
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_settings_trait_reaches_the_reply_policy() {
    let app = RustStream::new(AppInfo::new("mount-map-reply", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm_batch.batch(nonzero!(4)))
                .out(Reply, Prefixed::default())
                .prefixed("pre.");
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 5 })
        .to("mount.orders")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Order>("pre.mount.receipts")
        .assert_called_once()
        .with(&Order { id: 5 });
}

/// ...and one `Out` slot on a router, without a second implementation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_settings_trait_reaches_a_slot_policy() {
    let routes = Router::<MemoryBroker>::new()
        .include(mirror)
        .out(Audit, Prefixed::default())
        .prefixed("pre.")
        .build();
    let app = RustStream::new(AppInfo::new("mount-map-slot", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include_router(routes);
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 6 })
        .to("mount.mirror")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Order>("pre.mount.audit")
        .assert_called_once()
        .with(&Order { id: 6 });
}

/// The byte-for-byte reply: its bytes are the payload, so the wiring carries the publish policy
/// and nothing else.
#[subscriber("mount.raw", publish("mount.raw.receipts"))]
async fn echo_raw(order: &Order) -> Wire {
    Wire::of(order.id.to_be_bytes())
}

/// ...and the settings trait reaches that wiring too: a policy is all it carries, and replacing
/// it is what `map_publisher` does, so the position with no codec and no transform stack is
/// configured the same way as every other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_settings_trait_reaches_a_byte_for_byte_reply_policy() {
    let app = RustStream::new(AppInfo::new("mount-map-raw", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(echo_raw)
                .out(Reply, Prefixed::default())
                .prefixed("pre.");
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 4 })
        .to("mount.raw")
        .publish()
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .published::<Order>("pre.mount.raw.receipts")
        .assert_called_once()
        .with_raw(4u32.to_be_bytes().as_slice());
}

/// Where the two surfaces genuinely differ: a slot's publish pipeline is part of the
/// instantiated definition's type, so it is fixed when the slot binds. A scope binds inside the
/// app and its slots carry the app's `publish_layer` chain; a router is typed before the app
/// exists, so its slots publish with nothing in the way. This pins both halves, so the boundary
/// is asserted rather than assumed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scope_mounted_slot_carries_the_app_pipeline_and_a_router_mounted_one_does_not() {
    let app = RustStream::new(AppInfo::new("mount-slot-scope", "0.1.0"))
        .publish_layer(AppStamp)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(mirror).out(Audit, Publish).build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 1 })
        .to("mount.mirror")
        .publish()
        .await
        .expect("publish");

    tb.out::<Audit>()
        .assert_called_once()
        .with_header("x-app", b"1");
    tb.shutdown().await.expect("shutdown");

    let routes = Router::<MemoryBroker>::new()
        .include(mirror)
        .out(Audit, Publish)
        .build();
    let app = RustStream::new(AppInfo::new("mount-slot-router", "0.1.0"))
        .publish_layer(AppStamp)
        .with_broker(MemoryBroker::new(), |b| {
            b.include_router(routes);
        });
    let tb = TestApp::start(app).await.expect("harness start");

    tb.message(&Order { id: 2 })
        .to("mount.mirror")
        .publish()
        .await
        .expect("publish");

    let audit = tb.out::<Audit>().assert_called_once();
    assert_eq!(
        audit.messages()[0].headers().get("x-app"),
        None,
        "a router's slots are typed before the app that mounts them, so the app-wide publish \
         layer is not part of their pipeline",
    );
}
