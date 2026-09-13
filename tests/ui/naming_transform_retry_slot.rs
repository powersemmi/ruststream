use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, ForSlot, HandlerOutcome, Names, Outgoing as OutgoingMessage, PublishTransform, RustStream, SlotContext};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

struct ByTenant;

impl<Options> PublishTransform<ForSlot, Options> for ByTenant {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut OutgoingMessage<'_>,
        _options: &mut Option<Options>,
        _cx: &SlotContext<'_>,
    ) {
        out.set_name("orders.north");
    }
}

#[subscriber("orders")]
async fn reconcile(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

// The deferred copy goes to the address the subscription reported, so the retry slot has no
// destination to hand a transform: a transform that names one has nowhere to name it.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(reconcile)
            .out_retry(MemoryPublish)
            .transform(ByTenant);
    });
}
