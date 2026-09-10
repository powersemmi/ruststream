use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, ContextKind, HandlerOutcome, Names, Out, Outgoing as OutgoingMessage, PublishTransform, RustStream};
use ruststream::{OutSlot, Outgoing, TransactionalPublisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(Serialize, Outgoing)]
struct Audited {
    id: u32,
}

#[derive(OutSlot)]
#[publishes(Audited)]
struct Audit;

struct ByTenant;

impl<K: ContextKind> PublishTransform<K> for ByTenant {
    type Destination = Names;

    fn apply(&self, out: &mut OutgoingMessage<'_>, _cx: &K::View<'_>) {
        out.set_name("audit.north");
    }
}

// A transaction publishes straight to the broker, past the slot's publish path, so the redirect
// would never run on its messages. A redirected slot therefore offers plain sending and nothing
// else, and this handler asks for more.
#[subscriber("orders")]
async fn settle(
    order: &Order,
    Out(audit): Out<impl TransactionalPublisher, Audit>,
) -> HandlerOutcome {
    let Ok(scope) = audit.begin().await else {
        return HandlerOutcome::retry();
    };
    if scope
        .message(&Audited { id: order.id })
        .to("audit")
        .publish()
        .await
        .is_err()
        || scope.commit().await.is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(settle)
            .out(Audit, MemoryPublish)
            .transform(ByTenant)
            .build();
    });
}
