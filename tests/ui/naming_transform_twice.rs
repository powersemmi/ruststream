use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, ContextKind, ForReply, Names, Outgoing as OutgoingMessage, PublishContext, PublishTransform, Reply, RustStream};
use ruststream::{Outgoing, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(Serialize, Outgoing)]
struct Receipt {
    id: u32,
}

struct ReplyTo;

impl<C> PublishTransform<ForReply<C>> for ReplyTo {
    type Destination = Names;

    fn apply(&self, out: &mut OutgoingMessage<'_>, cx: &PublishContext<'_, C>) {
        out.set_name(cx.name().to_owned());
    }
}

struct Inbox;

impl<K: ContextKind> PublishTransform<K> for Inbox {
    type Destination = Names;

    fn apply(&self, out: &mut OutgoingMessage<'_>, _cx: &K::View<'_>) {
        out.set_name("inbox");
    }
}

#[subscriber("orders", publish("receipts"))]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

// A reply has one destination: the second redirect has no position left to fill.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm)
            .out(Reply, MemoryPublish)
            .transform(ReplyTo)
            .transform(Inbox);
    });
}
