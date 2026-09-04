use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{
    AppInfo, HandlerOutcome, Out, Outgoing, PublishContext, PublishTransform, RustStream, for_batch,
};
use ruststream::{OutSlot, Publisher, subscriber};
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(OutSlot)]
struct Audit;

struct Stamp;

impl<C> PublishTransform<C> for Stamp {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {
        out.headers_mut().insert("x-stamp", b"1".to_vec());
    }
}

#[subscriber("orders")]
async fn mirror(order: &Order, Out(_audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

// A slot publish is one message with no page, so a batch transform has nothing to run over: the
// step exists only on the reply position.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(mirror)
            .out(Audit, MemoryPublish)
            .batch_transform(for_batch(Stamp))
            .build();
    });
}
