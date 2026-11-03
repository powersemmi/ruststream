//! A function generic over `App` reads the service's metadata through the bound alone.

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{App, AppInfo, RustStream};

/// The title, the server names and the handler count of any app.
fn summary<A: App>(app: &A) -> (String, usize, usize) {
    (
        app.info().title.clone(),
        app.servers().len(),
        app.handlers().len(),
    )
}

/// A service whose composed type stays hidden behind `impl App`.
fn orders() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).register_broker(MemoryBroker::new())
}

#[test]
fn a_generic_bound_reads_the_metadata() {
    assert_eq!(summary(&orders()), ("orders".to_owned(), 0, 0));
}

#[test]
fn an_opaque_app_reads_the_metadata_without_an_import() {
    let app = orders();

    assert_eq!(app.info().title, "orders");
    assert!(app.servers().is_empty());
    assert!(app.handlers().is_empty());
}
