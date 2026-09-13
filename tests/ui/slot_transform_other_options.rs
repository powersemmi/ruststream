use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{
    AppInfo, ForSlot, HandlerOutcome, Out, Outgoing, PublishTransform, Reads, RustStream,
    SlotContext,
};
use ruststream::{OutSlot, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(Serialize, ruststream::Outgoing)]
struct Audited {
    id: u32,
}

#[derive(OutSlot)]
#[publishes(Audited)]
struct Audit;

/// Another broker's per-message settings.
#[derive(Clone, Default)]
struct Priority {
    level: Option<u8>,
}

// The transform writes `Priority`, so it belongs over a publisher whose `Publisher::Options` is
// `Priority`. The in-memory broker has no per-message setting at all (`Options = ()`).
struct Urgent;

impl PublishTransform<ForSlot, Priority> for Urgent {
    type Destination = Reads;

    fn apply(
        &self,
        _out: &mut Outgoing<'_>,
        options: &mut Option<Priority>,
        _cx: &SlotContext<'_>,
    ) {
        options.get_or_insert_with(Priority::default).level = Some(9);
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
            .transform(Urgent)
            .build();
    });
}
