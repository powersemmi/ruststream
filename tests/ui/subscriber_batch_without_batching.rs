use futures::Stream;
use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryError, MemorySubscriber};
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream, SubscriberSettings};
use ruststream::{Subscribe, Subscriber, SubscriptionSource, nonzero, subscriber};
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

/// A subscriber with no batching of its own: it forwards single deliveries and stops there.
struct OneAtATime(MemorySubscriber);

impl Subscriber for OneAtATime {
    type Message = <MemorySubscriber as Subscriber>::Message;
    type Error = <MemorySubscriber as Subscriber>::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.0.stream()
    }
}

#[derive(Clone)]
struct Trickle {
    name: &'static str,
}

impl SubscriptionSource<ConnectedMemoryBroker> for Trickle {
    type Subscriber = OneAtATime;

    fn name(&self) -> &str {
        self.name
    }

    async fn subscribe(self, connected: &ConnectedMemoryBroker) -> Result<OneAtATime, MemoryError> {
        Ok(OneAtATime(Subscribe::subscribe(connected, self.name).await?))
    }
}

// The signature asks for a page, and the mount names its size; this subscription cannot deliver
// pages at all, which is the broker's own gap to close.
#[subscriber(Trickle { name: "orders" })]
async fn handle(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
    HandlerOutcome::ack()
}

fn main() {
    let _app =
        RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(handle.batch(nonzero!(64)));
        });
}
