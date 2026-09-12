use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, ContextKind, HandlerOutcome, Names, Out, Outgoing as OutgoingMessage, PublishTransform, RustStream};
use ruststream::{OutSlot, Outgoing, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// The slot's dictionary fixes this message's channel, and the generated document reports it.
#[derive(Serialize, Outgoing)]
#[outgoing(name = "audit.orders")]
struct Audited {
    id: u32,
}

#[derive(OutSlot)]
#[publishes(Audited)]
struct Audit;

struct ByTenant;

impl<K: ContextKind, Options> PublishTransform<K, Options> for ByTenant {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut OutgoingMessage<'_>,
        _options: &mut Option<Options>,
        _cx: &K::View<'_>,
    ) {
        out.set_name("audit.north");
    }
}

#[subscriber("orders")]
async fn mirror(order: &Order, Out(audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    if audit.message(&Audited { id: order.id }).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(mirror)
            .out(Audit, MemoryPublish)
            .transform(ByTenant)
            .build();
    });
}
