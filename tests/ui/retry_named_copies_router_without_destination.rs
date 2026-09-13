use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{HandlerOutcome, Router};
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

// A router chain ends in `.build()`, so that is where a registration owing a destination is
// refused: the descriptor addresses no copies and nothing here names one.
fn main() {
    let _ = Router::<MemoryBroker>::new()
        .include(reconcile)
        .out_retry(MemoryPublish)
        .build();
}
