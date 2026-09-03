//! Delayed redelivery written without the `macros` feature: `retry_after` for the not-ready-yet
//! case, and per-element delays in a selective batch outcome.
//!
//! ```text
//! cargo run --example manual_retry --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Payment {
    id: u64,
    settled: bool,
}

// --8<-- [start:retry_after]
/// The not-ready-yet case: the upstream has not settled this payment, so an immediate
/// redelivery would just spin. Ask the broker to redeliver no sooner than five seconds from now.
struct Reconcile;

impl Handle<Payment> for Reconcile {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if !payment.settled {
            return ready(Err(HandlerOutcome::retry_after(Duration::from_secs(5))));
        }
        println!("payment {} settled", payment.id);
        ready(Ok(()))
    }
}
// --8<-- [end:retry_after]

// --8<-- [start:batch_retry_after]
/// Selective outcomes carry per-element delays: settled payments ack immediately, pending ones
/// come back in thirty seconds without holding up the rest of the page.
struct ReconcilePage;

impl Handle<[Payment]> for ReconcilePage {
    fn handle(
        &self,
        payments: &[Payment],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        ready(Err(payments
            .iter()
            .map(|payment| {
                if payment.settled {
                    HandlerOutcome::ack()
                } else {
                    HandlerOutcome::retry_after(Duration::from_secs(30))
                }
            })
            .collect()))
    }
}
// --8<-- [end:batch_retry_after]

fn app() -> RustStream {
    RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("payments", Reconcile).build());
        // Batches dispatch per page rather than per delivery, and the page input is what says so.
        b.include(subscriber("payments", ReconcilePage).build());
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
