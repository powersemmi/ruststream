use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{
    AppInfo, ForReply, Outgoing, PublishContext, PublishTransform, Reads, RustStream,
};
use ruststream::subscriber;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(Serialize, ruststream::Outgoing)]
struct Receipt {
    id: u32,
}

/// Another broker's per-message settings.
#[derive(Clone, Default)]
struct Priority {
    level: Option<u8>,
}

// The transform writes `Priority`, so it belongs over a publisher whose `Publisher::Options` is
// `Priority`. The in-memory broker has no per-message setting at all (`Options = ()`).
struct Urgent;

impl<C> PublishTransform<ForReply<C>, Priority> for Urgent {
    type Destination = Reads;

    fn apply(
        &self,
        _out: &mut Outgoing<'_>,
        options: &mut Option<Priority>,
        _cx: &PublishContext<'_, C>,
    ) {
        options.get_or_insert_with(Priority::default).level = Some(9);
    }
}

#[subscriber("orders", publish("receipts"))]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(confirm).out_reply(MemoryPublish).transform(Urgent);
    });
}
