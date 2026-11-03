//! The `AsyncAPI` document of a compio app is the document of the same app on tokio.

use ruststream::ServerSpec;
use ruststream::asyncapi;
use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::compio;
use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    #[allow(
        dead_code,
        reason = "the document is built from the type, nothing reads it"
    )]
    id: u64,
}

#[subscriber("orders")]
async fn orders(_order: &Order) {}

#[subscriber("returns")]
async fn returns(_order: &Order) {}

fn info() -> AppInfo {
    AppInfo::new("shop", "1.4.0").description("Order processing")
}

fn server() -> ServerSpec {
    ServerSpec::new("mem://local", "memory")
}

/// The app as a tokio service's `main` builds it: known only as `impl App`.
fn tokio_app() -> impl App {
    RustStream::new(info())
        .server("mem", server())
        .with_broker_codec(MemoryBroker::new(), JsonCodec, |b| {
            b.include(orders).include(returns)
        })
}

/// The same app as a compio service's `main` builds it.
fn compio_app() -> impl compio::App {
    compio::RustStream::new(info())
        .server("mem", server())
        .with_broker_codec(MemoryBroker::new(), JsonCodec, |b| {
            b.include(orders).include(returns)
        })
}

fn tokio_json() -> String {
    asyncapi::build_spec(&tokio_app())
        .to_json()
        .expect("the document serializes")
}

fn compio_json() -> String {
    asyncapi::build_spec(&compio_app())
        .to_json()
        .expect("the document serializes")
}

#[test]
fn a_compio_app_lists_the_channels_operations_and_servers_of_the_same_app_on_tokio() {
    let compio = compio_json();
    assert_eq!(compio, tokio_json());
    // The comparison is not between two empty documents.
    assert!(compio.contains("receive_orders"), "{compio}");
    assert!(compio.contains("receive_returns"), "{compio}");
    assert!(compio.contains("mem://local"), "{compio}");
}

/// The service the attribute generates a `main` for, as a binary writes it.
mod service {
    use std::process::ExitCode;

    use ruststream::codec::JsonCodec;
    use ruststream::memory::MemoryBroker;
    use ruststream::runtime::AppInfo;
    use ruststream::runtime::compio::{App, RustStream};

    use super::{orders, returns};

    #[ruststream::app(runtime = compio, cores = 2)]
    fn app() -> impl App {
        RustStream::new(AppInfo::new("shop", "1.4.0")).with_broker_codec(
            MemoryBroker::new(),
            JsonCodec,
            |b| b.include(orders).include(returns),
        )
    }

    #[test]
    fn the_compio_attribute_generates_a_main_that_returns_an_exit_code() {
        // Not called: the test process's arguments are not the service's.
        let entry: fn() -> ExitCode = main;
        let _ = entry;
    }
}
