//! The tutorial's service at step 4 without the `macros` feature: the reply handler mounted next
//! to the plain one, still without the router of step 5.
//!
//! ```text
//! cargo run --example manual_tutorial_reply_app --no-default-features --features memory,json,asyncapi,logging -- run
//! ```

mod orders;

use std::process::ExitCode;

use ruststream::memory::prelude::*;
use ruststream::runtime::cli::run_main;

// --8<-- [start:reply]
use crate::orders::{Confirm, Receive};

fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders-service", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Receive).build());
        // The definition names the reply's destination; with no `.out_reply(..)` at the mount
        // site the reply leaves through the broker's default publisher.
        b.include(
            subscriber("orders", Confirm)
                .reply()
                .to("confirmations")
                .build(),
        );
    })
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:reply]
