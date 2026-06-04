//! A minimal service wired with `#[ruststream::app]`: no hand-written `main`, no runtime setup.
//!
//! The attribute expands the builder below into a binary that understands `run` (the default) and
//! `asyncapi gen`. Try it with `cargo run --example macro_app --features macros,memory -- run`.

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, Context, HandlerMetadata, HandlerResult, RustStream};

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        let subscriber = b.broker().subscribe("orders");
        b.handle(
            subscriber,
            |_msg: &_, _ctx: &mut Context| async { HandlerResult::Ack },
            HandlerMetadata::raw("orders"),
        );
    })
}
