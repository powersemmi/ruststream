use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, Outgoing as OutgoingMessage, PublishContext, RedirectTransform, Reply, RustStream};
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

impl<C> RedirectTransform<C> for ReplyTo {
    fn apply(&self, out: &mut OutgoingMessage<'_>, cx: &PublishContext<'_, C>) {
        out.set_name(cx.name().to_owned());
    }
}

struct Inbox;

impl<C> RedirectTransform<C> for Inbox {
    fn apply(&self, out: &mut OutgoingMessage<'_>, _cx: &PublishContext<'_, C>) {
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
            .redirect(ReplyTo)
            .redirect(Inbox);
    });
}
