//! The services of `examples/seek.rs` written without the `macros` feature: `start_at(..)` is a
//! step of the mount chain, and a body that declares the broker's `MemoryContext` reads the seek
//! handle from its context.
//!
//! ```text
//! cargo test --example manual_seek --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

mod start_at {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde::{Deserialize, Serialize};

    // --8<-- [start:start_at]
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

    // --8<-- [start:retaining]
    /// Replaying reads what the broker kept, so this one keeps the last 64 entries of every name.
    /// `MemoryBroker::new()` keeps nothing, and a mount that seeks does not compile on it.
    fn broker() -> MemoryBroker<Retaining> {
        MemoryBroker::retaining(Retention::Messages(nonzero!(64)))
    }
    // --8<-- [end:retaining]

    fn app(broker: MemoryBroker<Retaining>) -> impl App {
        RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
            // The audit trail opens at the start of the log, so it also reads what came before it.
            b.include(
                subscriber("audit", Record)
                    .start_at(MemoryPosition::start())
                    .build(),
            );
        })
    }
    // --8<-- [end:start_at]

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

    #[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
    struct Job {
        id: u64,
    }

    // `#[outgoing(name = "jobs")]` by hand: the type fixes its destination.
    impl OutgoingDestination for Job {
        type Form = FixedName;

        const DESTINATION: &'static str = "jobs";
    }

    impl MessageHeaders for Job {
        type Contract = NoHeaders;
    }

    struct Work;

    // Declaring the broker's `MemoryContext` is what puts the seek handle in the context.
    impl Handle<Job, (), (), MemoryContext> for Work {
        async fn handle(
            &self,
            job: &Job,
            _outs: &(),
            ctx: &mut Context<'_, MemoryContext>,
        ) -> Result<(), HandlerOutcome> {
            if job.id == POISON {
                // Resume at the fourth entry of the log.
                let seeker = ctx.context(SeekHandle);
                if seeker.seek(MemoryPosition::sequence(3)).await.is_err() {
                    return Err(HandlerOutcome::retry());
                }
                return Ok(());
            }
            tracing::info!(job.id, "processed");
            Ok(())
        }
    }

    fn app(broker: MemoryBroker<Retaining>) -> impl App {
        RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(broker, |b| {
            b.include(
                subscriber("jobs", Work)
                    .start_at(MemoryPosition::start())
                    .build(),
            );
        })
    }
    // --8<-- [end:seek]

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
