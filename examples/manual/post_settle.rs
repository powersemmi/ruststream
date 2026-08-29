//! Post-settle continuations written without the `macros` feature: `HandlerResult::ack()
//! .and_after(..)` attaches a side effect that runs after the message is settled, without gating
//! the ack decision or affecting redelivery. The batch form attaches one continuation per element.
//!
//! ```text
//! cargo run --example manual_post_settle --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::runtime::{
    BatchResult, Handler, HandlerMetadata, RouterDef, Settle, SliceHandler, typed,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:single]
/// Ack the order, then fire a non-critical follow-up once it is acknowledged. The continuation is
/// at-most-once: if it is lost or panics, the already-acked order is not redelivered.
///
/// `and_after` already produces a `Settle`, which is what the trait method returns, so this is the
/// one outcome shape that needs no `.into()`. The handler itself awaits nothing - the continuation
/// is handed over, not run here - so it returns the future directly.
struct Handle;

impl Handler<Order> for Handle {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        let id = order.id;
        ready(HandlerResult::ack().and_after(async move {
            println!("order {id} acked; notifying downstream");
        }))
    }
}
// --8<-- [end:single]

// --8<-- [start:batch]
/// Per-element settlement: id 0 retries with no continuation, every other order acks and schedules
/// its own follow-up. The continuation rides with the element, so a batch settles each message and
/// its side effect independently.
struct HandlePage;

impl SliceHandler<Order> for HandlePage {
    fn handle_slice(
        &self,
        orders: &[Order],
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = BatchResult> + Send {
        ready(BatchResult::PerElement(
            orders
                .iter()
                .map(|order| {
                    if order.id == 0 {
                        HandlerResult::retry().into()
                    } else {
                        let id = order.id;
                        HandlerResult::ack().and_after(async move {
                            println!("order {id} acked in batch; following up");
                        })
                    }
                })
                .collect(),
        ))
    }
}
// --8<-- [end:batch]

/// Batches dispatch per page, so they register through `subscribe_batch` on a router; a
/// `BrokerScope` attaches single-delivery handlers only.
fn batch_routes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new()
        .with_codec(JsonCodec)
        .subscribe_batch(
            Name::new("orders"),
            HandlePage,
            HandlerMetadata::typed::<Order>("orders"),
        )
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("post_settle", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.subscribe(
            Name::new("orders"),
            typed(JsonCodec, Handle),
            HandlerMetadata::typed::<Order>("orders"),
        );
        b.include_router(batch_routes());
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
