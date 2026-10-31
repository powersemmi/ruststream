// The refused item expands to nothing, so the imports it would use stay unused.
#![allow(unused_imports)]

use ruststream::app;
use ruststream::runtime::{AppInfo, RustStream};

// The runtime an app runs on is named by the attribute: `tokio` or `embassy`.
#[app(runtime = smol)]
fn build() -> RustStream {
    RustStream::new(AppInfo::new("svc", "0.1.0"))
}

fn main() {}
