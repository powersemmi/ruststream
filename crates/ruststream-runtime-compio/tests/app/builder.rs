//! A mounted handler is described, the metadata reaches `Described`, and the placement of the
//! mounts over the cores fails at startup where the `threads` mounts do not fit.

use std::error::Error;
use std::fmt;
use std::future::ready;
use std::num::NonZeroUsize;

use ruststream_core::ServerSpec;
use ruststream_memory::local::LocalMemoryBroker;
use ruststream_memory::{MemoryBroker, MemorySource};
use ruststream_runtime::{
    AppInfo, Context, Described, Deserialized, Handle, HandlerOutcome, Input, SoloDeserialized,
    Workers, subscriber,
};
use ruststream_runtime_compio::{App, Cores, RustStream, RustStreamError};

/// A reading: the delivery's bytes, decoded by no codec.
struct Reading<'a>(#[allow(dead_code)] &'a [u8]);

#[derive(Debug)]
struct Unreadable;

impl fmt::Display for Unreadable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unreadable")
    }
}

impl Error for Unreadable {}

impl Deserialized for Reading<'_> {
    type Output<'a> = Reading<'a>;
    type Error = Unreadable;

    fn from_payload(payload: &[u8]) -> Result<Reading<'_>, Unreadable> {
        Ok(Reading(payload))
    }
}

impl Input for Reading<'_> {
    type Axis = SoloDeserialized<Reading<'static>>;
}

struct Accept;

impl Handle<Reading<'_>> for Accept {
    fn handle(
        &self,
        _reading: &Reading<'_>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        ready(Ok(()))
    }
}

fn nonzero(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).expect("a count")
}

fn orders() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber(MemorySource::new("orders"), Accept).build())
    })
}

fn pools() -> RustStream {
    RustStream::new(AppInfo::new("pools", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber(MemorySource::new("audit"), Accept).build())
            .include(subscriber(MemorySource::new("ledger"), Accept).build())
            .workers(Workers::threads(nonzero(2)))
            .include(subscriber(MemorySource::new("mail"), Accept).build())
            .workers(Workers::threads(nonzero(2)))
    })
}

#[test]
fn a_mounted_handler_is_described() {
    let app = orders();
    let names: Vec<&str> = app.handlers().iter().map(|meta| &*meta.name).collect();
    assert_eq!(names, ["orders"]);
}

#[test]
fn the_info_and_the_servers_reach_described() {
    let app = RustStream::new(AppInfo::new("draft", "0.0.1"))
        .with_info(AppInfo::new("orders", "0.1.0"))
        .server("main", ServerSpec::new("localhost:4222", "nats"))
        .register_broker(MemoryBroker::new());
    let described: &dyn Described = &app;
    assert_eq!(described.described_info().title, "orders");
    assert_eq!(
        described.described_servers().keys().collect::<Vec<_>>(),
        ["main"]
    );
    assert!(described.described_handlers().is_empty());
}

#[test]
fn threads_mounts_that_fit_the_cores_are_placed() {
    let layout = pools().layout(5).expect("four dedicated cores fit five");
    assert_eq!(layout.cores(0), [0]);
    assert_eq!(layout.cores(1), [1, 2]);
    assert_eq!(layout.cores(2), [3, 4]);
}

#[test]
fn threads_mounts_past_the_cores_fail_naming_the_mounts() {
    let error = pools()
        .layout(4)
        .expect_err("four dedicated cores need five");
    let text = error.to_string();
    assert!(
        text.contains("`ledger`: 2") && text.contains("`mail`: 2"),
        "{text}"
    );
}

/// The title, the server names and the handler count of any app.
fn summary<A: App>(app: &A) -> (String, usize, usize) {
    (
        app.info().title.clone(),
        app.servers().len(),
        app.handlers().len(),
    )
}

/// A service whose composed type stays hidden behind `impl App`.
fn hidden_app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).register_broker(MemoryBroker::new())
}

#[test]
fn a_generic_bound_reads_the_metadata() {
    assert_eq!(summary(&hidden_app()), ("orders".to_owned(), 0, 0));
}

#[test]
fn an_opaque_app_reads_the_metadata_without_an_import() {
    let app = hidden_app();

    assert_eq!(app.info().title, "orders");
    assert!(app.servers().is_empty());
    assert!(app.handlers().is_empty());
}

/// An app on the broker that needs no reactor, which the tests below run.
fn local_app() -> impl App {
    RustStream::new(AppInfo::new("local", "0.1.0")).register_broker(LocalMemoryBroker::<8>::new())
}

/// Serves any app until its shutdown signal, the way a generic launcher holds it.
fn serve_until_told<A: App>(app: A) -> Result<(), RustStreamError> {
    app.run_until(
        Cores::new(nonzero(1)).unpinned().without_signals(),
        ready(()),
    )
}

#[test]
fn an_app_behind_the_trait_runs_until_its_signal() {
    serve_until_told(local_app()).expect("the run ends cleanly");
}

#[test]
fn an_app_behind_the_trait_starts_and_shuts_down() {
    super::support::on_runtime(async {
        let running = local_app().start().await.expect("the service starts");
        running.shutdown().await.expect("a clean shutdown");
    });
}

#[test]
fn an_app_is_shown_with_its_title_and_what_it_mounts() {
    let shown = format!("{:?}", orders());
    assert!(
        shown.contains("orders") && shown.contains("handlers: 1"),
        "{shown}"
    );
}
