//! A batch of carried values mounts in the plain batch form only: a reply on it does not compile.
use ruststream::nonzero;
use ruststream::memory::{MemoryBroker, MemorySource};
use ruststream::prelude::*;
use ruststream::runtime::{Input, SoloCarried};
use serde::Serialize;

#[derive(Clone)]
struct Row {
    id: u64,
}

impl Input for Row {
    type Axis = SoloCarried<Row>;
}

#[derive(Serialize, Outgoing)]
#[outgoing(name = "seen")]
struct Seen {
    id: u64,
}

#[subscriber(MemorySource::new("rows"), reply)]
async fn handle(rows: &[Row]) -> Vec<Seen> {
    rows.iter().map(|row| Seen { id: row.id }).collect()
}

fn main() {
    let _ = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle.batch(nonzero!(8)));
    });
}
