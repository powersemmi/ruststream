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

use common::Order;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{Stream, StreamExt};
use ruststream::codec::JsonCodec;
use ruststream::memory::{MemoryBroker, MemoryMessage, MemorySubscriber};
use ruststream::runtime::{
    AppInfo, Context, HandlerMetadata, HandlerOutcome, RustStream, Typed, typed,
};
use ruststream::testing::TestApp;
use ruststream::{AckError, HeaderMap, IncomingMessage, Subscriber};

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

/// Runs one delivery of `Order` through `handler`, mounted on the counting subscription, and
/// answers how many times the runtime asked the delivery for its headers.
async fn header_reads<H>(handler: H) -> usize
where
    H: ruststream::runtime::Handler<CountingMessage> + 'static,
{
    let reads = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&reads);
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), move |b| {
            let subscriber = CountingSubscriber {
                inner: b.broker().subscribe("orders"),
                reads: counter,
            };
            b.handle(subscriber, handler, HandlerMetadata::raw("orders"));
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_decoded_handler_never_asks_the_delivery_for_its_headers() {
    let reads = header_reads(typed(JsonCodec, |order: &Order, _ctx: &mut Context| {
        assert_eq!(order.id, 7);
        async { HandlerOutcome::ack() }
    }))
    .await;

    assert_eq!(
        reads, 0,
        "a handler over a decoded payload reads no headers, so none are materialized"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_byte_lane_handler_never_asks_the_delivery_for_its_headers() {
    /// The lane marker: the view stays `&[u8]`, so nothing decodes and nothing reads a header.
    struct Frame;

    let reads = header_reads(Typed::<
        CountingMessage,
        ruststream::runtime::Provided<Frame>,
        (),
        _,
    >::over((), |bytes: &[u8], _ctx: &mut Context| {
        assert!(!bytes.is_empty());
        async { HandlerOutcome::ack() }
    }))
    .await;

    assert_eq!(
        reads, 0,
        "the byte lane hands the payload over untouched and never reaches the header map"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_that_reads_the_headers_materializes_them_once() {
    let reads = header_reads(typed(JsonCodec, |_order: &Order, ctx: &mut Context| {
        assert!(ctx.headers().is_empty());
        // Twice on purpose: the second read answers from the resolved map.
        assert!(ctx.headers().is_empty());
        async { HandlerOutcome::ack() }
    }))
    .await;

    assert_eq!(
        reads, 1,
        "the delivery is asked once however often the handler reads the map"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_that_enriches_the_headers_asks_the_delivery_once() {
    let reads = header_reads(typed(JsonCodec, |_order: &Order, ctx: &mut Context| {
        ctx.headers_mut().insert("x-seen", "yes");
        assert!(ctx.headers().get("x-seen").is_some());
        async { HandlerOutcome::ack() }
    }))
    .await;

    assert_eq!(
        reads, 1,
        "the working copy is cloned from the delivery's map, which is resolved once"
    );
}
