//! A `with_broker` closure that returns something that is not a mount chain does not compile,
//! with the message the embedded runner gives for the same mistake.

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};

fn main() {
    let _app = RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(MemoryBroker::new(), |_b| ());
}
