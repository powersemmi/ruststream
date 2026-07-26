//! A global layer added after `with_broker` could not wrap the already-registered handlers, so
//! the builder leaves the phase where `layer` exists and this must not compile.
use ruststream::memory::MemoryBroker;
use ruststream::runtime::layers::TracingLayer;
use ruststream::runtime::{AppInfo, RustStream};

fn main() {
    let _app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |_b| {})
        .layer(TracingLayer::default());
}
