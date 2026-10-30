use ruststream_memory::local::LocalMemoryBroker;
use ruststream_runtime_embassy::{EmbeddedApp, Mounts};

fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |_b| ())
}

fn main() {
    let _ = app(LocalMemoryBroker::new());
}
