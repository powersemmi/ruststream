//! The sealed `App` trait: a builder returns `impl App`, and the service runs and exposes its
//! AsyncAPI metadata through the trait without naming the concrete `RustStream<L, St, PP>` type.
#![cfg(all(feature = "macros", feature = "memory", feature = "json"))]

use std::sync::Arc;

use ruststream::ServerSpec;
use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream::subscriber;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Serialize, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(_order: &Order) {}

// The concrete return type would be `RustStream<Identity, (), PublishIdentity>` with a server and a
// handler baked in; `impl App` hides it. Returning the trait is the whole point of the sugar.
fn build() -> impl App {
    RustStream::new(AppInfo::new("svc", "0.2.0").with_description("demo"))
        .server("mem", ServerSpec::new("mem://local", "memory"))
        .with_broker_codec(MemoryBroker::new(), JsonCodec, |b| {
            b.include(handle);
        })
}

#[test]
fn metadata_is_readable_through_the_trait() {
    let app = build();
    assert_eq!(app.info().title, "svc");
    assert_eq!(app.info().version, "0.2.0");
    assert_eq!(app.info().description.as_deref(), Some("demo"));
    assert!(app.servers().contains_key("mem"));
    assert_eq!(app.handlers().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_impl_app_runs_and_shuts_down_through_the_trait() {
    let shutdown = Arc::new(Notify::new());
    let signal = Arc::clone(&shutdown);
    // `run_until` is reached only through `App` here; spawning it proves the future is `Send`.
    let run = tokio::spawn(build().run_until(async move { signal.notified().await }));
    shutdown.notify_one();
    run.await.expect("join").expect("run");
}

#[cfg(feature = "asyncapi")]
#[test]
fn build_spec_reads_an_impl_app() {
    let spec = ruststream::asyncapi::build_spec(&build());
    assert_eq!(spec.info.title, "svc");
    assert!(spec.servers.contains_key("mem"));
    assert!(spec.channels.contains_key("orders"));
}
