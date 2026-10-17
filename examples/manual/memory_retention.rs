//! The service of `examples/memory_retention.rs` without the `macros` feature: `start_at(..)` is
//! a step of the `subscriber` chain.
//!
//! ```text
//! cargo test --example manual_memory_retention --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the service; the test that runs it exists only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Entry {
    id: u64,
}

// `#[outgoing(name = "audit")]` by hand: the type fixes its destination.
impl OutgoingDestination for Entry {
    type Form = FixedName;

    const DESTINATION: &'static str = "audit";
}

impl MessageHeaders for Entry {
    type Contract = NoHeaders;
}

// --8<-- [start:app]
/// Keeps the newest two messages of every name.
fn broker() -> MemoryBroker<Retaining> {
    MemoryBroker::retaining(Retention::Messages(nonzero!(2)))
}

struct Record;

impl Handle<Entry> for Record {
    fn handle(
        &self,
        entry: &Entry,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(entry.id, "recorded");
        ready(Ok(()))
    }
}

fn app(broker: MemoryBroker<Retaining>) -> impl App {
    RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
        // Opens at the oldest message still kept, so it reads what came before the service.
        b.include(
            subscriber("audit", Record)
                .start_at(MemoryPosition::start())
                .build(),
        );
    })
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
