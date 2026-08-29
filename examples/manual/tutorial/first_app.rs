//! The tutorial's service at step 3 without the `macros` feature: one handler registered straight
//! on the broker scope, before the reply of step 4 and the router of step 5 arrive.
//!
//! ```text
//! cargo run --example manual_tutorial_first_app --no-default-features --features memory,json,asyncapi
//! ```

// Step 3 registers `Handle` alone; the module's reply types stay unused until step 4.
#[allow(dead_code)]
// --8<-- [start:app]
mod orders;

use std::error::Error;

use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::runtime::{HandlerMetadata, typed};

use crate::orders::{Handle, Order};

fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders-service", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.subscribe(
            Name::new("orders"),
            typed(JsonCodec, Handle),
            HandlerMetadata::typed::<Order>("orders"),
        );
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
// --8<-- [end:app]
