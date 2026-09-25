//! A `retry_after` copy the runtime publishes itself survives a graceful shutdown that comes
//! inside its delay.
//!
//! The runtime drops the original delivery when it arms the copy's timer, so a timer that dies
//! with the service loses the message. The subject is the shutdown sequence, which the test
//! harness does not run, so the app is started and shut down for real over `MemoryBroker`, and
//! the copy is read off the broker.
#![cfg(all(feature = "memory", feature = "macros", feature = "json"))]

mod common;

use std::convert::Infallible;
use std::future::{Future, ready};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use common::Order;
use futures::{Stream, StreamExt};
use ruststream::memory::prelude::*;
use ruststream::{
    AckError, AddressedCopies, HeaderMap, IncomingMessage, RedeliveryAddress, RedeliveryAddressed,
    Subscribe, Subscriber, SubscriptionSource,
};
use tokio::sync::Notify;
use tokio::time::timeout;

/// How long the test waits for the copy.
const DEADLINE: Duration = Duration::from_secs(10);

/// A subscription whose deliveries report no native delayed redelivery, so a `retry_after` takes
/// the runtime's own copy path; the copy goes to `copies`, where the test reads it.
#[derive(Debug, Clone)]
struct CopiedSubscription;

impl CopiedSubscription {
    const fn new() -> Self {
        Self
    }
}

impl<C: Subscribe> SubscriptionSource<C> for CopiedSubscription {
    type Subscriber = UnsettledSubscriber<C::Subscriber>;
    type Copies = AddressedCopies;

    // The returned lifetime is fixed by the trait, so it cannot be narrowed to `&'static str`.
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "retried"
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(UnsettledSubscriber(connected.subscribe("retried").await?))
    }
}

impl<C: Subscribe> RedeliveryAddressed<C> for CopiedSubscription {
    fn redelivery_address(
        &self,
        _connected: &C,
    ) -> impl Future<Output = Result<RedeliveryAddress, C::Error>> + Send {
        ready(Ok(RedeliveryAddress::new("copies")))
    }
}

/// The broker's subscriber with its native delayed redelivery taken away.
struct UnsettledSubscriber<S>(S);

impl<S: Subscriber> Subscriber for UnsettledSubscriber<S> {
    type Message = UnsettledMessage<S::Message>;
    type Error = S::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.0.stream().map(|item| item.map(UnsettledMessage))
    }
}

/// A delivery that settles like the broker's own but keeps the trait default for
/// [`IncomingMessage::supports_nack_after`].
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

/// Defers every delivery by `300 ms` and says so.
#[subscriber(CopiedSubscription::new(), workers(2))]
async fn retried(_order: &Order, ctx: &mut Context<'_, (), Arc<Notify>>) -> HandlerOutcome {
    ctx.state().notify_one();
    HandlerOutcome::retry_after(Duration::from_millis(300))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copy_pending_at_shutdown_is_published() {
    let broker = MemoryBroker::new();
    let mut copies = broker.subscribe("copies");
    let mut arrivals = pin!(copies.stream());
    let deferred = Arc::new(Notify::new());
    let state = Arc::clone(&deferred);
    let app = RustStream::new(AppInfo::new("retry", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker.clone(), |b| {
            b.include(retried).out_retry(Publish);
        });
    let running = app.start().await.expect("startup");
    broker
        .publisher()
        .message(&Order { id: 1 })
        .to("retried")
        .publish()
        .await
        .expect("publish");
    timeout(DEADLINE, deferred.notified())
        .await
        .expect("the handler defers the delivery");
    // Inside the delay: the original is already dropped, the copy is only a timer.
    running.shutdown().await.expect("shutdown");
    let copy = timeout(DEADLINE, arrivals.next()).await;
    assert!(
        matches!(copy, Ok(Some(Ok(_)))),
        "the copy pending at shutdown was lost"
    );
}
