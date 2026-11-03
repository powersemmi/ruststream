//! A retaining in-memory broker: it keeps the newest messages of every name within a bound, and a
//! subscription that opens at the start of the log replays what is kept.
//!
//! ```text
//! cargo test --example memory_retention --features testing,macros,memory,json
//! ```

// A plain build compiles the service; the test that runs it exists only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "audit")]
struct Entry {
    id: u64,
}

// --8<-- [start:app]
/// Keeps the newest two messages of every name.
fn broker() -> MemoryBroker<Retaining> {
    MemoryBroker::retaining(Retention::Messages(nonzero!(2)))
}

/// Opens at the oldest message still kept, so it reads what came before the service.
#[subscriber("audit", start_at(MemoryPosition::start()))]
async fn record(entry: &Entry) -> HandlerOutcome {
    tracing::info!(entry.id, "recorded");
    HandlerOutcome::ack()
}

fn app(broker: MemoryBroker<Retaining>) -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| b.include(record))
}
// --8<-- [end:app]

// The scenario is the test below; the command in the header runs it.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Entry, app, broker};

    #[tokio::test]
    async fn only_the_newest_two_are_replayed() -> Result<(), Box<dyn Error>> {
        let broker = broker();
        // Published before the service starts: the bound evicts the first.
        let publisher = broker.publisher();
        for id in [1, 2, 3] {
            publisher.message(&Entry { id }).publish().await?;
        }
        let tb = TestApp::start(app(broker)).await?;
        tb.settle().await?;

        let ids: Vec<u64> = tb
            .broker::<MemoryBroker<Retaining>>()
            .subscriber("audit")
            .received::<Entry>()
            .iter()
            .map(|entry| entry.id)
            .collect();
        assert_eq!(ids, [2, 3]);
        Ok(())
    }
}
