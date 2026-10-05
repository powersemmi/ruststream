//! A batch handler taking values of the carried lane, mounted on a subscription whose batches lend
//! nothing: the error names the subscription and the type.
use ruststream::memory::{MemoryBroker, MemorySource};
use ruststream::nonzero;
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
async fn handle(rows: &[Row]) -> HandlerOutcome {
    let _ = rows.iter().map(|row| row.id).sum::<u64>();
    HandlerOutcome::ack()
}

fn main() {
    let _ = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle.batch(nonzero!(8)));
    });
}
