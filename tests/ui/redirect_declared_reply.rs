use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, Outgoing as OutgoingMessage, PublishContext, PublishTransform, Reply, RustStream};
use ruststream::{Outgoing, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// The reply type names its own channel, and the generated document reports that channel. A
// redirect would publish it somewhere else, so the step is not available on this reply.
#[derive(Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u32,
}

struct ReplyTo;

impl<C> PublishTransform<C> for ReplyTo {
    fn apply(&self, out: &mut OutgoingMessage<'_>, cx: &PublishContext<'_, C>) {
        out.set_name(cx.name().to_owned());
    }
}

#[subscriber("orders", publish)]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm).out(Reply, MemoryPublish).redirect(ReplyTo);
    });
}
