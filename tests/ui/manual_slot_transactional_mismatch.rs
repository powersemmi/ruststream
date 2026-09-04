use ruststream::memory::{MemoryBroker, MemoryRequest};
use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, schemars::JsonSchema)]
struct Order {
    id: u32,
}

struct Journal;

impl OutSlot for Journal {
    const NAME: &'static str = "Journal";
}

// The body bounds the entry's wired value with the transactional capability, but the include
// site binds the marker to a policy whose live publisher (the memory requester) has no
// transactions: the mount fails to compile with the capability diagnostic, naming the marker.
struct Record;

impl<J> Handle<Order, (), Outs<(J,)>> for Record
where
    J: OutEntry<Journal, Wire: TransactionalPublisher>,
{
    async fn handle(
        &self,
        order: &Order,
        _outs: &Outs<(J,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let _ = order.id;
        Ok(())
    }
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Record).build())
            .out(Journal, MemoryRequest)
            .build();
    });
}
