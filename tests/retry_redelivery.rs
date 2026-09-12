//! Where the deferred `retry_after` fallback publishes its copy.
//!
//! The subscription in these suites is shaped like a Google Pub/Sub one: it has a name of its
//! own, its deliveries flow from a separate topic, and only a publish to that topic reaches it
//! again. No broker in this crate is built that way, so the shape is assembled here over the
//! in-memory broker: the topic is the memory subject, the subscription name is what the
//! registration is known by, and the delivery reports no native delayed redelivery, which is what
//! puts the runtime's fallback on the retry path.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::future::{Future, ready};
use std::time::Duration;

use common::Order;
use futures::{Stream, StreamExt};
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, RETRY_COUNT_HEADER, RustStream};
use ruststream::subscriber;
use ruststream::testing::{Outcome, TestApp};
use ruststream::{
    AckError, HeaderMap, IncomingMessage, RedeliveryAddress, Subscribe, Subscriber,
    SubscriptionSource,
};

const RETRY_DELAY: Duration = Duration::from_secs(5);

/// A subscription whose name is not a publish destination: deliveries come from `topic`, and a
/// publish reaches it only there.
#[derive(Debug, Clone)]
struct BoundSubscription {
    subscription: &'static str,
    topic: &'static str,
}

impl BoundSubscription {
    const fn new(subscription: &'static str, topic: &'static str) -> Self {
        Self {
            subscription,
            topic,
        }
    }
}

impl<C: Subscribe> SubscriptionSource<C> for BoundSubscription {
    type Subscriber = UnsettledSubscriber<C::Subscriber>;

    fn name(&self) -> &str {
        self.subscription
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(UnsettledSubscriber(connected.subscribe(self.topic).await?))
    }

    fn redelivery_address(
        &self,
        _connected: &C,
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, C::Error>> + Send {
        // The descriptor already holds the topic, so no lookup stands between it and the answer.
        ready(Ok(Some(RedeliveryAddress::new(self.topic))))
    }
}

/// The same subscription shape from a broker crate that has not adopted the contract: it reports
/// no redelivery address at all.
#[derive(Debug, Clone)]
struct SilentSubscription {
    topic: &'static str,
}

impl SilentSubscription {
    const fn new(topic: &'static str) -> Self {
        Self { topic }
    }
}

impl<C: Subscribe> SubscriptionSource<C> for SilentSubscription {
    type Subscriber = UnsettledSubscriber<C::Subscriber>;

    fn name(&self) -> &str {
        self.topic
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(UnsettledSubscriber(connected.subscribe(self.topic).await?))
    }
}

/// The broker's subscriber with its native delayed redelivery taken away, so a `retry_after`
/// takes the runtime's deferred re-publish instead of the broker's own timer.
struct UnsettledSubscriber<S>(S);

impl<S: Subscriber> Subscriber for UnsettledSubscriber<S> {
    type Message = UnsettledMessage<S::Message>;
    type Error = S::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.0.stream().map(|item| item.map(UnsettledMessage))
    }
}

/// A delivery that acknowledges like the broker's own but keeps the trait default for
/// [`IncomingMessage::supports_nack_after`], which is what nearly every real broker ships.
struct UnsettledMessage<M>(M);

impl<M: IncomingMessage> IncomingMessage for UnsettledMessage<M> {
    fn payload(&self) -> &[u8] {
        self.0.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.0.headers()
    }

    async fn ack(self) -> Result<(), AckError> {
        self.0.ack().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.0.nack(requeue).await
    }
}

/// Defers the first delivery of every message, acks the copy that comes back.
#[subscriber(BoundSubscription::new("orders-workers", "orders"))]
async fn reconcile(order: &Order, ctx: &mut Context) -> HandlerOutcome {
    let attempt = ctx
        .headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    if attempt == 0 {
        assert_eq!(order.id, 1);
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// The deferred copy goes to the address the source reported, so the handler sees the message
/// again. Published under the subscription's own name it would reach nothing, and the delayed
/// message would be lost.
#[tokio::test(start_paused = true)]
async fn a_deferred_retry_reaches_the_handler_through_the_reported_address() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.retry_via(MemoryPublish);
            b.include(reconcile);
        },
    );
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.broker::<MemoryBroker>()
        .subscriber("orders-workers")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    // The delay is real: nothing comes back before it elapses.
    tb.advance(RETRY_DELAY.saturating_sub(Duration::from_millis(1)))
        .await
        .expect("settle");
    tb.broker::<MemoryBroker>()
        .subscriber("orders-workers")
        .assert_called_once();

    tb.advance(Duration::from_millis(1)).await.expect("settle");
    assert_eq!(
        tb.broker::<MemoryBroker>()
            .subscriber("orders-workers")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the deferred copy must reach the handler and settle",
    );
}

/// Defers every delivery. Mounted on a source that reports no redelivery address, so the app it
/// belongs to never starts.
#[subscriber(SilentSubscription::new("payments"))]
async fn settle_later(_order: &Order) -> HandlerOutcome {
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// A scope that wires a deferred-retry publisher over a subscription that cannot say where a
/// redelivery is published fails to start, naming the subscription, its source and the fix.
#[tokio::test]
async fn a_retry_publisher_over_an_unaddressed_source_refuses_to_start() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.retry_via(MemoryPublish);
            b.include(settle_later);
        },
    );

    let failed = TestApp::start(app)
        .await
        .expect_err("a scope that cannot address its retries must not start");
    let message = failed.to_string();
    assert!(message.contains("payments"), "{message}");
    assert!(message.contains("SilentSubscription"), "{message}");
    assert!(message.contains("MemoryBroker"), "{message}");
    assert!(message.contains("retry_via"), "{message}");
}

/// Without a retry publisher the same subscription starts: `retry_after` then degrades to an
/// immediate requeue, which is the documented answer for a broker with neither native delayed
/// redelivery nor a publisher to defer through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unaddressed_source_starts_when_the_scope_defers_nothing() {
    let app = RustStream::new(AppInfo::new("redelivery", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(settle_later));

    let tb = TestApp::start(app).await.expect("startup failed");
    tb.shutdown().await.expect("graceful shutdown failed");
}
