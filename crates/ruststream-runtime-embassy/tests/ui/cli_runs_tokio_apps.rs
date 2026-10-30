use ruststream_memory::local::LocalMemoryBroker;
use ruststream_runtime_embassy::{EmbeddedApp, Mounts};

fn app() -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(LocalMemoryBroker::<4>::new(), |b| b)
}

fn main() {
    let _ = ruststream_cli::run_main(app);
}
