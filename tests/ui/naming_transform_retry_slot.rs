use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{
    AppInfo, ForReply, HandlerOutcome, Names, Outgoing as OutgoingMessage, PublishContext,
    PublishTransform, RustStream,
};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

struct ByTenant;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ByTenant {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut OutgoingMessage<'_>,
        _options: &mut Option<Options>,
        _cx: &PublishContext<'_, C>,
    ) {
        out.set_name("orders.north");
    }
}

#[subscriber("orders")]
async fn reconcile(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

// The by-name descriptor addresses its own copies on this broker, so the position has a
// destination already: a transform that names one has nothing to name here.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(reconcile)
            .out_retry(MemoryPublish)
            .transform(ByTenant);
    });
}
