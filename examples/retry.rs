//! Delayed redelivery from the Subscribers guide: `retry_after` for the not-ready-yet case,
//! and per-element delays in a selective batch outcome.
//!
//! ```text
//! cargo run --example retry --features macros,memory,json -- run
//! ```

use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Payment {
    id: u64,
    settled: bool,
}

// --8<-- [start:retry_after]
/// The not-ready-yet case: the upstream has not settled this payment, so an immediate
/// redelivery would just spin. Ask the broker to redeliver no sooner than five seconds from now.
#[subscriber("payments")]
async fn reconcile(payment: &Payment) -> HandlerResult {
    if !payment.settled {
        return HandlerResult::retry_after(Duration::from_secs(5));
    }
    println!("payment {} settled", payment.id);
    HandlerResult::Ack
}
// --8<-- [end:retry_after]

// --8<-- [start:batch_retry_after]
/// Selective outcomes carry per-element delays: settled payments ack immediately, pending ones
/// come back in thirty seconds without holding up the rest of the page.
#[subscriber(batch("payments"))]
async fn reconcile_page(payments: &[Payment]) -> Vec<HandlerResult> {
    payments
        .iter()
        .map(|payment| {
            if payment.settled {
                HandlerResult::Ack
            } else {
                HandlerResult::retry_after(Duration::from_secs(30))
            }
        })
        .collect()
}
// --8<-- [end:batch_retry_after]

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(reconcile);
        b.include(reconcile_page);
    })
}
