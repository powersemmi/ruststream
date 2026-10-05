//! A handler taking a value of the carried lane, mounted on a subscription whose deliveries carry
//! nothing: the error names the subscription and the type.
use ruststream::memory::{MemoryBroker, MemorySource};
use ruststream::prelude::*;
use ruststream::runtime::{Input, SoloCarried};

#[derive(Clone)]
struct Row {
    id: u64,
}

// What a broker crate's derive writes for its row type.
impl Input for Row {
    type Axis = SoloCarried<Row>;
}

#[subscriber(MemorySource::new("rows"))]
async fn handle(row: &Row) -> HandlerOutcome {
    let _ = row.id;
    HandlerOutcome::ack()
}

fn main() {
    let _ = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle);
    });
}
