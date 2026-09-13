use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
use ruststream::{NamedCopies, Subscribe, SubscriptionSource, subscriber};
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

/// A subscription that reads many destinations and addresses none of them.
#[derive(Clone)]
struct Filter;

impl<C: Subscribe> SubscriptionSource<C> for Filter {
    type Subscriber = C::Subscriber;
    type Copies = NamedCopies;

    fn name(&self) -> &str {
        "sensors.+"
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        connected.subscribe("sensors.+").await
    }
}

#[subscriber(Filter {})]
async fn reconcile(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

// The descriptor addresses no copies, so the registration owes a destination: neither `.to(..)`
// nor a naming transform names one here.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(reconcile).out_retry(MemoryPublish);
    });
}
