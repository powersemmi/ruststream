//! Start positions and seeking on a broker that keeps a log: a subscription that opens at the
//! start of the log replays what was published before the service started, and a handler that
//! repositions its own subscription skips a poisoned region. Each scenario is a test.
//!
//! ```text
//! cargo test --example seek --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

mod start_at {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    // --8<-- [start:start_at]
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "audit")]
    struct Entry {
        id: u64,
    }

    /// The audit trail opens at the start of the log, so it also reads what came before it.
    #[subscriber("audit", start_at(MemoryPosition::start()))]
    async fn record(entry: &Entry) -> HandlerOutcome {
        tracing::info!(entry.id, "recorded");
        HandlerOutcome::ack()
    }

    // --8<-- [start:retaining]
    /// Replaying reads what the broker kept, so this one keeps the last 64 entries of every name.
    /// `MemoryBroker::new()` keeps nothing, and a mount that seeks does not compile on it.
    fn broker() -> MemoryBroker<Retaining> {
        MemoryBroker::retaining(Retention::Messages(nonzero!(64)))
    }
    // --8<-- [end:retaining]
    // --8<-- [end:start_at]

    fn app(broker: MemoryBroker<Retaining>) -> impl App {
        RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| b.include(record))
    }

    #[tokio::test]
    async fn the_history_is_replayed() -> Result<(), Box<dyn Error>> {
        let broker = broker();
        // Published before the service starts.
        let publisher = broker.publisher();
        for id in [1, 2] {
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
        assert_eq!(ids, [1, 2]);
        Ok(())
    }
}

mod seek {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    // --8<-- [start:seek]
    /// The id that marks a poisoned region: the entries up to the resume point are skipped.
    const POISON: u64 = 999;

    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "jobs")]
    struct Job {
        id: u64,
    }

    /// The seek handle is a field of the broker's per-delivery context, read with `Ctx`.
    #[subscriber("jobs", start_at(MemoryPosition::start()))]
    async fn work(job: &Job, Ctx(seeker): Ctx<SeekHandle>) -> HandlerOutcome {
        if job.id == POISON {
            // Resume at the fourth entry of the log.
            if seeker.seek(MemoryPosition::sequence(3)).await.is_err() {
                return HandlerOutcome::retry();
            }
            return HandlerOutcome::ack();
        }
        tracing::info!(job.id, "processed");
        HandlerOutcome::ack()
    }
    // --8<-- [end:seek]

    fn app(broker: MemoryBroker<Retaining>) -> impl App {
        RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| b.include(work))
    }

    #[tokio::test]
    async fn the_poisoned_region_is_skipped() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(64)));
        let publisher = broker.publisher();
        for id in [1, POISON, 3, 4] {
            publisher.message(&Job { id }).publish().await?;
        }
        let tb = TestApp::start(app(broker)).await?;
        tb.settle().await?;

        let ids: Vec<u64> = tb
            .broker::<MemoryBroker<Retaining>>()
            .subscriber("jobs")
            .received::<Job>()
            .iter()
            .map(|job| job.id)
            .collect();
        assert_eq!(ids, [1, POISON, 4]);
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
