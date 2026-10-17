//! Poll diagnostics: every subscription times its handler's polls, and the reports name the
//! handlers that compute on the app's runtime. Each scenario is a test.
//!
//! ```text
//! cargo test --example poll_diagnostics --features testing,macros,memory,json,poll-diagnostics
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, ruststream::Outgoing)]
struct Order {
    id: u64,
}

/// Stands in for real work: five milliseconds of computation that hold the thread.
fn compute(order: &Order) -> u64 {
    let until = Instant::now() + Duration::from_millis(5);
    let mut acc = order.id;
    while Instant::now() < until {
        acc = acc.wrapping_mul(31).wrapping_add(1);
    }
    acc
}

mod diagnostics {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::memory::prelude::*;
    use ruststream::runtime::{PollDiagnostics, PollReport};
    use ruststream::testing::TestApp;

    use super::{Order, compute};

    /// Waits on nothing: each poll is short.
    #[subscriber("orders")]
    async fn accept(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "accepted");
        HandlerOutcome::ack()
    }

    /// Computes for five milliseconds in one poll: it belongs on `threads(n)`.
    #[subscriber("reports")]
    async fn summarize(order: &Order) -> HandlerOutcome {
        let total = compute(order);
        tracing::info!(order.id, total, "summarized");
        HandlerOutcome::ack()
    }

    // --8<-- [start:diagnostics]
    /// The app gets the settings; the caller keeps a clone of `diagnostics` to read the reports.
    fn app(diagnostics: &PollDiagnostics) -> impl App {
        let diagnostics = diagnostics
            .clone()
            .threshold(Duration::from_millis(1))
            .sample_every(nonzero!(8u32));
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .poll_diagnostics(diagnostics)
            .with_broker(MemoryBroker::new(), |b| {
                b.include(accept);
                b.include(summarize);
            })
    }
    // --8<-- [end:diagnostics]

    // --8<-- [start:reports]
    /// A periodic task beside the app: the handlers whose average poll is over `limit`.
    fn slow_handlers(diagnostics: &PollDiagnostics, limit: Duration) -> Vec<PollReport> {
        diagnostics
            .reports()
            .into_iter()
            .filter(|report| report.average() > limit)
            .collect()
    }
    // --8<-- [end:reports]

    #[tokio::test]
    async fn the_computing_handler_is_reported() -> Result<(), Box<dyn Error>> {
        let diagnostics = PollDiagnostics::new();
        let tb = TestApp::start(app(&diagnostics)).await?;
        let broker = tb.broker::<MemoryBroker>();
        for id in 1..=8 {
            let order = Order { id };
            broker.message(&order).to("orders").publish().await?;
            broker.message(&order).to("reports").publish().await?;
        }

        // One poll in eight is timed: eight deliveries, one sample each.
        let orders = diagnostics
            .report("orders")
            .ok_or("orders was not sampled")?;
        assert_eq!(orders.samples(), 1);
        let slow: Vec<String> = slow_handlers(&diagnostics, Duration::from_millis(1))
            .iter()
            .map(|report| report.subscription().to_owned())
            .collect();
        assert_eq!(slow, ["reports"]);
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
