//! The tutorial's service at step 4 without the `macros` feature: the reply handler mounted next
//! to the plain one, still without the router of step 5.
//!
//! ```text
//! cargo run --example manual_tutorial_reply_app --no-default-features --features memory,json,asyncapi
//! ```

mod orders;

use std::error::Error;

use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::runtime::{HandlerMetadata, typed};

// --8<-- [start:reply]
use crate::orders::{Confirm, Handle, Order};

fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders-service", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.subscribe(
            Name::new("orders"),
            typed(JsonCodec, Handle),
            HandlerMetadata::typed::<Order>("orders"),
        );
        // The reply definition carries its own subject, and with no `.publisher(..)` chained the
        // reply leaves through the broker's default publisher.
        b.include(Confirm);
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
// --8<-- [end:reply]
