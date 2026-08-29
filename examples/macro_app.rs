//! A minimal service wired with `#[ruststream::app]`: no hand-written `main`, no runtime setup.
//!
//! The attribute expands the builder below into a binary that understands `run` (the default) and
//! `asyncapi gen`. Try it with `cargo run --example macro_app --features macros,memory -- run`.

use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // `raw` skips the decode step, so this service needs no codec feature at all.
        b.include(raw("orders", |_payload: &[u8], _ctx: &mut Context| async {
            HandlerResult::Ack
        }));
    })
}
