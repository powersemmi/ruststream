//! A minimal service wired with `#[ruststream::app]`: no hand-written `main`, no runtime setup.
//!
//! The attribute expands the builder below into a binary that understands `run` (the default) and
//! `asyncapi gen`. Try it with `cargo run --example macro_app --features macros,memory -- run`.

use std::future::{Future, ready};

use ruststream::memory::prelude::*;

/// The raw input type: the derive gives the newtype the delivery's bytes as they arrive.
#[derive(Deserialized)]
struct RawOrder<'a>(&'a [u8]);

/// A raw-payload body: `RawOrder` borrows the delivery's bytes, so this service needs no codec
/// feature at all.
struct Ingest;

impl<'p> Handle<RawOrder<'p>> for Ingest {
    fn handle(
        &self,
        order: &RawOrder<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = order.0.len();
        ready(Ok(()))
    }
}

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Ingest).build());
    })
}
