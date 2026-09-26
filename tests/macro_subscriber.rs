//! Integration test for the `#[subscriber]` attribute macro.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

use std::convert::Infallible;
use std::future::{Future, ready};

use ruststream::memory::prelude::*;
use ruststream::memory::{ConnectedMemoryBroker, MemorySubscriber};
use ruststream::runtime::{
    ContextKind, Outgoing, PublishLayer, PublishNext, PublishTransform, Reads,
};
use ruststream::testing::{Outcome, TestApp};
use ruststream::{
    AddressedCopies, RedeliveryAddress, RedeliveryAddressed, Subscribe, SubscriptionSource,
};
use serde::{Deserialize, Serialize};

// The derive is spelled out: `runtime::Outgoing` above is the publish transform's message view,
// a different item that happens to share the name.
#[derive(Debug, PartialEq, Serialize, Deserialize, ruststream::Outgoing)]
struct Order {
    id: u32,
    total: f64,
}

/// A broker-specific subscription descriptor (stand-in for e.g. a Redis stream), named by the
/// definition itself. Proves a macro def works on an arbitrary `SubscriptionSource`, not just a
/// topic string. `Clone` because the mount rebuilds the source from the definition's settings
/// builder, the way a broker descriptor is cloned per registration.
#[derive(Clone)]
struct StreamSource {
    name: String,
}

impl StreamSource {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
        }
    }

    /// A fluent option returning `Self`, so a `StreamSource::new(..).at(..)` chain can sit directly
    /// in the `#[subscriber(..)]` decorator.
    fn at(mut self, name: &str) -> Self {
        name.clone_into(&mut self.name);
        self
    }
}

impl SubscriptionSource<ConnectedMemoryBroker> for StreamSource {
    type Subscriber = MemorySubscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedMemoryBroker,
    ) -> Result<MemorySubscriber, MemoryError> {
        Subscribe::subscribe(connected, &self.name).await
    }
}

// One in-memory subject is both what the subscription reads and what a publish reaches, and no
// lookup stands between the descriptor and the answer.
impl RedeliveryAddressed<ConnectedMemoryBroker> for StreamSource {
    fn redelivery_address(
        &self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, MemoryError>> + Send {
        ready(Ok(RedeliveryAddress::new(self.name.clone())))
    }
}

// A builder chain in the decorator: the macro follows the receivers down to `StreamSource::new`
// for the type, and emits the whole chain as the source constructor.
#[subscriber(StreamSource::new("placeholder").at("chain.stream"))]
async fn on_chain(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn macro_builder_chain_in_decorator() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(on_chain);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    // The `at(..)` option won: the subscription binds to "chain.stream".
    tb.message(&Order { id: 7, total: 1.0 })
        .to("chain.stream")
        .publish()
        .await
        .expect("publish failed");

    tb.broker::<MemoryBroker>()
        .subscriber("chain.stream")
        .assert_called_once()
        .with(&Order { id: 7, total: 1.0 })
        .settled(HandlerOutcome::ack());
}

/// An order placed by a customer.
#[derive(MessageInfo)]
#[allow(dead_code)]
struct DescribedOrder {
    id: u32,
}

#[test]
fn derive_message_metadata() {
    assert_eq!(DescribedOrder::NAME, "DescribedOrder");
    assert_eq!(
        DescribedOrder::DESCRIPTION,
        Some("An order placed by a customer."),
    );
}

/// A static (zero-cost) publish transform composed onto the reply wiring.
struct StaticEnvelope;

impl<K: ContextKind, Options> PublishTransform<K, Options> for StaticEnvelope {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &K::View<'_>) {
        out.headers_mut().insert("x-static", b"1".to_vec());
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ruststream::Outgoing)]
struct Ping {
    n: u32,
}

#[subscriber("ping-in", publish("ping-out"))]
async fn relay(p: &Ping) -> Ping {
    Ping { n: p.n }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_publish_layer_transforms_reply() {
    // The static layer is composed onto the policy stack at compile time - no dyn dispatch.
    let egress = MemoryBroker::new().bindable();
    let egress_pub = egress.bind(Publish);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker_labeled("egress", egress, |_b| {})
        .with_broker_labeled("ingress", MemoryBroker::new(), |b| {
            b.include(relay)
                .out(Reply, egress_pub)
                .transform(StaticEnvelope);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker_named("ingress")
        .message(&Ping { n: 7 })
        .to("ping-in")
        .publish()
        .await
        .expect("publish failed");

    // The static publish layer stamped the reply on its way to the other broker.
    tb.broker_named("egress")
        .published::<Ping>("ping-out")
        .assert_called_once()
        .with(&Ping { n: 7 })
        .with_header("x-static", b"1");
}

#[derive(Serialize, Deserialize, ruststream::Outgoing)]
struct Request {
    n: u32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ruststream::Outgoing)]
struct Response {
    doubled: u32,
}

/// A publish middleware that tags every outgoing reply with a header (envelope-style).
#[derive(Clone)]
struct Tagger;

impl PublishLayer for Tagger {
    async fn on_publish<'a, N: ruststream::runtime::PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        out.headers_mut().insert("x-envelope", b"1".to_vec());
        next.run(out).await
    }
}

#[subscriber("requests", publish("responses"))]
async fn reply(req: &Request) -> Response {
    Response { doubled: req.n * 2 }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn macro_publisher_replies_cross_broker() {
    // The reply is published cross-broker: a token bound to egress; name from the macro.
    let egress = MemoryBroker::new().bindable();
    let egress_pub = egress.bind(Publish);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .publish_layer(Tagger)
        .with_broker_labeled("egress", egress, |_b| {})
        .with_broker_labeled("ingress", MemoryBroker::new(), |b| {
            b.include(reply).out(Reply, egress_pub);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.broker_named("ingress")
        .message(&Request { n: 21 })
        .to("requests")
        .publish()
        .await
        .expect("publish failed");

    tb.broker_named("egress")
        .published::<Response>("responses")
        .assert_called_once()
        .with(&Response { doubled: 42 })
        .with_header("x-envelope", b"1");
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ruststream::Outgoing)]
struct Confirmation {
    id: u32,
    accepted: bool,
}

#[subscriber("confirm-in", publish("confirm-out"))]
async fn confirm(order: &Order) -> Result<Confirmation, HandlerOutcome> {
    if order.id == 0 {
        return Err(HandlerOutcome::drop());
    }
    Ok(Confirmation {
        id: order.id,
        accepted: true,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_result_form_controls_ack_and_publish() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm);
    });
    let tb = TestApp::start(app).await.expect("startup failed");

    // Err(HandlerOutcome) skips the publish entirely.
    tb.message(&Order { id: 0, total: 0.0 })
        .to("confirm-in")
        .publish()
        .await
        .expect("publish failed");

    tb.broker::<MemoryBroker>()
        .subscriber("confirm-in")
        .assert_called_once()
        .assert_outcome(Outcome::Drop);
    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("confirm-out")
        .assert_not_called();

    // Ok(reply) publishes and acks.
    tb.message(&Order { id: 6, total: 1.0 })
        .to("confirm-in")
        .publish()
        .await
        .expect("publish failed");

    tb.broker::<MemoryBroker>()
        .published::<Confirmation>("confirm-out")
        .assert_called_once()
        .with(&Confirmation {
            id: 6,
            accepted: true,
        });
}

/// App state read from the publishing handler's optional `&mut Context` parameter.
#[derive(Clone, Copy)]
struct Bump(u32);

#[subscriber("ctx-in", publish("ctx-out"))]
async fn ctx_reply(req: &Request, ctx: &mut Context<'_, (), Bump>) -> Response {
    let bump = ctx.state().0;
    Response {
        doubled: req.n + bump,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_handler_reads_context_state() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Bump(100)))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(ctx_reply);
        });
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Request { n: 1 })
        .to("ctx-in")
        .publish()
        .await
        .expect("publish failed");

    // 1 plus the state's bump: the publishing handler read app state from the context.
    tb.broker::<MemoryBroker>()
        .published::<Response>("ctx-out")
        .assert_called_once()
        .with(&Response { doubled: 101 });
}
