use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{AppInfo, ContextKind, HandlerOutcome, Out, Outgoing, PublishTransform, Reads, RustStream, for_batch};
use ruststream::{OutSlot, Publisher, subscriber};
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(OutSlot)]
struct Audit;

struct Stamp;

impl<K: ContextKind, Options> PublishTransform<K, Options> for Stamp {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        _cx: &K::View<'_>,
    ) {
        out.headers_mut().insert("x-stamp", b"1".to_vec());
    }
}

#[subscriber("orders")]
async fn mirror(order: &Order, Out(_audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

// A slot publish is one message with no batch, so a batch transform has nothing to run over: the
// step exists only on the reply position.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(mirror)
            .out(Audit, MemoryPublish)
            .batch_transform(for_batch(Stamp))
            .build();
    });
}
