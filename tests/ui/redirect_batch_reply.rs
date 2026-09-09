use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, ForReply, Outgoing as OutgoingMessage, PublishContext, PublishTransform, Reply, RustStream, SubscriberSettings};
use ruststream::{Outgoing, nonzero, subscriber};
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
    fn apply(&self, out: &mut OutgoingMessage<'_>, cx: &PublishContext<'_, C>) {
        out.set_name(cx.name().to_owned());
    }
}

#[subscriber("orders", publish("receipts"))]
async fn confirm(orders: &[Order]) -> Vec<Receipt> {
    orders.iter().map(|o| Receipt { id: o.id }).collect()
}

// A batch's replies are published against the batch, which answers many deliveries and carries
// none of their headers: there is nothing for a redirect to read.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm.batch(nonzero!(8)))
            .out(Reply, MemoryPublish)
            .redirect(ReplyTo);
    });
}
