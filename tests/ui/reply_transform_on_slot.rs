use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{
    AppInfo, ForReply, HandlerOutcome, Out, Outgoing as OutgoingMessage, PublishContext,
    PublishTransform, RustStream,
};
use ruststream::{OutSlot, Outgoing, Publisher, subscriber};
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

// The transform reads the delivery, so it is a reply transform. A slot publish is issued by the
// handler body and has no delivery to hand on, so this one has no place on a slot.
struct StampSource;

impl<C> PublishTransform<ForReply<C>> for StampSource {
    fn apply(&self, out: &mut OutgoingMessage<'_>, cx: &PublishContext<'_, C>) {
        out.headers_mut()
            .insert("x-source", cx.name().as_bytes().to_vec());
    }
}

#[subscriber("orders")]
async fn mirror(order: &Order, Out(audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    let sent = audit
        .message(&Audited { id: order.id })
        .to("audit")
        .publish()
        .await;
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(mirror)
            .out(Audit, MemoryPublish)
            .transform(StampSource)
            .build();
    });
}
