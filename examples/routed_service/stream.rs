//! A broker-specific subscription descriptor - the kind a real broker crate (a Redis stream, a NATS
//! `JetStream` consumer) ships. It carries more than a name (here a consumer group) and knows how to
//! open the subscription against a connected broker.
//!
//! Used straight in the decorator: `#[subscriber(OrdersStream::new("orders", "workers"))]`. The
//! macro reads the `OrdersStream` type out of the constructor call, so the compiler checks the
//! descriptor against the broker it is mounted on.

use std::convert::Infallible;

use ruststream::SubscriptionSource;
use ruststream::memory::{MemoryBroker, MemorySubscriber};

/// A stream subscription consumed under a named group.
///
/// The group is illustrative: the in-memory broker subscribes by name only, but a real broker would
/// use it to join a consumer group. This is exactly where broker-specific config lives instead of
/// leaking into the framework.
pub(crate) struct OrdersStream {
    name: String,
    group: String,
}

impl OrdersStream {
    /// Creates a stream source bound to `name`, consumed under `group`.
    pub(crate) fn new(name: &str, group: &str) -> Self {
        Self {
            name: name.to_owned(),
            group: group.to_owned(),
        }
    }
}

impl SubscriptionSource<MemoryBroker> for OrdersStream {
    type Subscriber = MemorySubscriber;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(self, broker: &MemoryBroker) -> Result<MemorySubscriber, Infallible> {
        let _ = &self.group; // a real broker would join the consumer group here
        Ok(broker.subscribe(&self.name))
    }
}
