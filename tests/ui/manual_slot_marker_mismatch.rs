use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Deserialize, schemars::JsonSchema)]
struct Order {
    id: u32,
}

struct Audit;

impl OutSlot for Audit {
    const NAME: &'static str = "Audit";
}

struct Journal;

impl OutSlot for Journal {
    const NAME: &'static str = "Journal";
}

// The body's arena entry is bound to the `Audit` marker, but the include site attaches its
// policy to `Journal`: the entry the mount builds is not the one the body declared, and the
// mismatch is a compile error naming both markers.
struct Record;

impl<A> Handle<Order, (), Outs<(A,)>> for Record
where
    A: OutEntry<Audit, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        _outs: &Outs<(A,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let _ = order.id;
        Ok(())
    }
}

fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Record).build())
            .out(Journal, MemoryPublish)
            .build();
    });
}
