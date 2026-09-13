use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
use ruststream::{BrokerMoves, Subscribe, SubscriptionSource, subscriber};
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

/// A queue the broker itself moves a spent delivery out of, so this process publishes no copy
/// for it.
#[derive(Clone)]
struct ManagedQueue;

impl<C: Subscribe> SubscriptionSource<C> for ManagedQueue {
    type Subscriber = C::Subscriber;
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        "orders"
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        connected.subscribe("orders").await
    }
}

#[subscriber(ManagedQueue {})]
async fn reconcile(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

// The broker moves the delivery itself, so there is no publisher of this process's to name.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(reconcile).out_retry(MemoryPublish);
    });
}
