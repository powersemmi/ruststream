//! `on_startup` fixes the app's state type; handlers already registered against another state
//! type could not be carried across, so after `with_broker` the method no longer exists.
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};

fn main() {
    let _app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |_b| {})
        .on_startup(async move |()| Ok::<_, std::io::Error>(42_u32));
}
