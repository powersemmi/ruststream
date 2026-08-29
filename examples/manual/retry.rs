//! Delayed redelivery written without the `macros` feature: `retry_after` for the not-ready-yet
//! case, and per-element delays in a selective batch outcome.
//!
//! ```text
//! cargo run --example manual_retry --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::time::Duration;

use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::runtime::{
    BatchResult, Handler, HandlerMetadata, RouterDef, Settle, SliceHandler, typed,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Payment {
    id: u64,
    settled: bool,
}

// --8<-- [start:retry_after]
/// The not-ready-yet case: the upstream has not settled this payment, so an immediate
/// redelivery would just spin. Ask the broker to redeliver no sooner than five seconds from now.
struct Reconcile;

impl Handler<Payment> for Reconcile {
    fn handle(
        &self,
        payment: &Payment,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Settle> + Send {
        if !payment.settled {
            return ready(HandlerResult::retry_after(Duration::from_secs(5)).into());
        }
        println!("payment {} settled", payment.id);
        ready(HandlerResult::ack().into())
    }
}
// --8<-- [end:retry_after]

// --8<-- [start:batch_retry_after]
/// Selective outcomes carry per-element delays: settled payments ack immediately, pending ones
/// come back in thirty seconds without holding up the rest of the page.
struct ReconcilePage;

impl SliceHandler<Payment> for ReconcilePage {
    fn handle_slice(
        &self,
        payments: &[Payment],
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = BatchResult> + Send {
        ready(BatchResult::PerElement(
            payments
                .iter()
                .map(|payment| {
                    if payment.settled {
                        HandlerResult::ack().into()
                    } else {
                        HandlerResult::retry_after(Duration::from_secs(30)).into()
                    }
                })
                .collect(),
        ))
    }
}
// --8<-- [end:batch_retry_after]

/// Batches dispatch per page, so they register through `subscribe_batch` on a router; a
/// `BrokerScope` attaches single-delivery handlers only.
fn batch_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new()
        .with_codec(JsonCodec)
        .subscribe_batch(
            Name::new("payments"),
            ReconcilePage,
            HandlerMetadata::typed::<Payment>("payments"),
        )
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.subscribe(
            Name::new("payments"),
            typed(JsonCodec, Reconcile),
            HandlerMetadata::typed::<Payment>("payments"),
        );
        b.include_router(batch_routes());
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
