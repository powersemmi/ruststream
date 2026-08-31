//! The tutorial's service at step 4 without the `macros` feature: the reply handler mounted next
//! to the plain one, still without the router of step 5.
//!
//! ```text
//! cargo run --example manual_tutorial_reply_app --no-default-features --features memory,json,asyncapi
//! ```

mod orders;

use std::error::Error;

use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;

// --8<-- [start:reply]
use crate::orders::{Confirm, Receive};

fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders-service", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Receive).build());
        // The reply names its destination on the chain, and with no `.publisher(..)` chained
        // the reply leaves through the broker's default publisher.
        b.include(
            subscriber("orders", Confirm)
                .reply()
                .on("confirmations")
                .build(),
        );
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
// --8<-- [end:reply]
