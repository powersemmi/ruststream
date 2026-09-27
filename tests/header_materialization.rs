//! What a delivery is asked for on its way to a handler: the header map is a broker accessor, and
//! on several transports answering it costs a buffer, a parse and its destruction. A handler that
//! reads no headers must not pay for them, and one that reads them must pay once.
//!
//! The in-memory broker's map is built before the delivery leaves it, so the count is what these
//! suites assert rather than the time: a delivery of its own counts the calls the runtime makes.
#![cfg(all(
    feature = "memory",
    feature = "json",
    feature = "macros",
    feature = "testing"
))]

mod common;

use common::{Order, Receipt};
use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{Stream, StreamExt};
use ruststream::memory::{MemoryBroker, MemoryMessage, MemoryPublish, MemorySubscriber};
use ruststream::runtime::{
    AppInfo, Context, ForReply, Handle, HandlerOutcome, IntoSource, Outgoing, PublishContext,
    PublishTransform, Reads, RustStream, subscriber,
};
use ruststream::testing::TestApp;
use ruststream::{
    AckError, AddressedCopies, Deserialized, HeaderMap, IncomingMessage, RedeliveryAddress,
    RedeliveryAddressed, Subscribe, Subscriber, SubscriptionSource,
};

/// A delivery that counts what the runtime asks of it. Everything else is the in-memory broker's
/// own message, so the dispatch, the settle and the harness assertions are the real ones.
struct CountingMessage {
    inner: MemoryMessage,
    reads: Arc<AtomicUsize>,
}

impl IncomingMessage for CountingMessage {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.headers()
    }

    async fn ack(self) -> Result<(), AckError> {
        self.inner.ack().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.inner.nack(requeue).await
    }
}

/// The subscription behind the counting delivery: the broker's own stream, wrapped one message at
/// a time.
struct CountingSubscriber {
    inner: MemorySubscriber,
    reads: Arc<AtomicUsize>,
}

impl Subscriber for CountingSubscriber {
    type Message = CountingMessage;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let reads = Arc::clone(&self.reads);
        self.inner.stream().map(move |item| {
            let reads = Arc::clone(&reads);
            item.map(|inner| CountingMessage { inner, reads })
        })
    }
}

/// Mounts `handler` on the counting subscription, runs one delivery of `Order` through it, and
/// answers how many times the runtime asked the delivery for its headers.
macro_rules! header_reads {
    ($handler:expr) => {{
        let reads = Arc::new(AtomicUsize::new(0));
        let counted = Counted {
            name: "orders",
            reads: Arc::clone(&reads),
        };
        let app =
            RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
                b.include(subscriber(counted, $handler).build());
            });

        let tb = TestApp::start(app).await.expect("startup failed");
        tb.message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await
            .expect("publish");
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called(1)
            .settled(HandlerOutcome::ack());

        reads.load(Ordering::Relaxed)
    }};
}

/// Takes the decoded order and nothing else.
struct ReadsNothing;

impl Handle<Order> for ReadsNothing {
    fn handle(
        &self,
        _order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_decoded_handler_never_asks_the_delivery_for_its_headers() {
    let reads = header_reads!(ReadsNothing);

    assert_eq!(
        reads, 0,
        "a handler over a decoded payload reads no headers, so none are materialized"
    );
}

/// The payload view of the byte lane: the view stays the delivery's bytes, so nothing decodes and
/// nothing reads a header.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

/// Takes the delivery's bytes and nothing else.
struct ReadsBytes;

impl<'p> Handle<Frame<'p>> for ReadsBytes {
    fn handle(
        &self,
        frame: &Frame<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = frame.0;
        ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_byte_lane_handler_never_asks_the_delivery_for_its_headers() {
    let reads = header_reads!(ReadsBytes);

    assert_eq!(
        reads, 0,
        "the byte lane hands the payload over untouched and never reaches the header map"
    );
}

/// Reads the header map twice.
struct ReadsTwice;

impl Handle<Order> for ReadsTwice {
    fn handle(
        &self,
        _order: &Order,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        // Twice on purpose: the second read answers from the resolved map.
        let reads = [ctx.headers().is_empty(), ctx.headers().is_empty()];
        ready(if reads == [true, true] {
            Ok(())
        } else {
            Err(HandlerOutcome::drop())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_that_reads_the_headers_materializes_them_once() {
    let reads = header_reads!(ReadsTwice);

    assert_eq!(
        reads, 1,
        "the delivery is asked once however often the handler reads the map"
    );
}

/// Writes a header into the working copy and reads it back.
struct Enriches;

impl Handle<Order> for Enriches {
    fn handle(
        &self,
        _order: &Order,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ctx.headers_mut().insert("x-seen", "yes");
        ready(if ctx.headers().get("x-seen").is_some() {
            Ok(())
        } else {
            Err(HandlerOutcome::drop())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_that_enriches_the_headers_asks_the_delivery_once() {
    let reads = header_reads!(Enriches);

    assert_eq!(
        reads, 1,
        "the working copy is cloned from the delivery's map, which is resolved once"
    );
}

/// The counting subscription as a descriptor, so a mount site that names a reply can take it.
#[derive(Clone)]
struct Counted {
    name: &'static str,
    reads: Arc<AtomicUsize>,
}

impl<Connected> SubscriptionSource<Connected> for Counted
where
    Connected: Subscribe<Subscriber = MemorySubscriber>,
{
    type Subscriber = CountingSubscriber;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(
        self,
        connected: &Connected,
    ) -> Result<CountingSubscriber, Connected::Error> {
        Ok(CountingSubscriber {
            inner: connected.subscribe(self.name).await?,
            reads: self.reads,
        })
    }
}

impl<Connected> RedeliveryAddressed<Connected> for Counted
where
    Connected: Subscribe<Subscriber = MemorySubscriber>,
{
    fn redelivery_address(
        &self,
        _connected: &Connected,
    ) -> impl Future<Output = Result<RedeliveryAddress, Connected::Error>> + Send {
        // One subject is both ends of the in-memory bus.
        ready(Ok(RedeliveryAddress::new(self.name)))
    }
}

impl IntoSource for Counted {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

/// Answers every order with its receipt and reads nothing else.
struct Confirm;

impl Handle<Order, Receipt> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { id: order.id }))
    }
}

/// Copies the delivery's tenant onto the reply: a transform that reads the delivery's headers.
struct CarryTenant;

impl<C, Options> PublishTransform<ForReply<C>, Options> for CarryTenant {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(tenant) = cx.headers().get_shared("x-tenant") {
            out.headers_mut().insert("x-tenant", tenant);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_no_transform_reads_never_asks_the_delivery_for_its_headers() {
    let reads = Arc::new(AtomicUsize::new(0));
    let counted = Counted {
        name: "orders",
        reads: Arc::clone(&reads),
    };
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber(counted, Confirm).reply().to("receipts").build())
            .out_reply(MemoryPublish);
    });

    let tb = TestApp::start(app).await.expect("startup failed");
    tb.message(&Order { id: 7 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 7 });

    assert_eq!(
        reads.load(Ordering::Relaxed),
        0,
        "a reply whose transforms read no header leaves the delivery's map unasked"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_transform_that_reads_the_headers_asks_the_delivery_once() {
    let reads = Arc::new(AtomicUsize::new(0));
    let counted = Counted {
        name: "orders",
        reads: Arc::clone(&reads),
    };
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber(counted, Confirm).reply().to("receipts").build())
            .out_reply(MemoryPublish)
            .transform(CarryTenant);
    });

    let tb = TestApp::start(app).await.expect("startup failed");
    let mut tenant = HeaderMap::new();
    tenant.insert("x-tenant", "acme");
    tb.message(&Order { id: 7 })
        .to("orders")
        .with_headers(tenant)
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with_header("x-tenant", "acme");

    assert_eq!(
        reads.load(Ordering::Relaxed),
        1,
        "the transform reads the delivery's map, which is asked for once"
    );
}
