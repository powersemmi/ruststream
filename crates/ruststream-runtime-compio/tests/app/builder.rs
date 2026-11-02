//! A mounted handler is described, the metadata reaches `Described`, and the placement of the
//! mounts over the cores fails at startup where the `threads` mounts do not fit.

use std::error::Error;
use std::fmt;
use std::future::ready;
use std::num::NonZeroUsize;

use ruststream_core::ServerSpec;
use ruststream_memory::{MemoryBroker, MemorySource};
use ruststream_runtime::{
    AppInfo, Context, Described, Deserialized, Handle, HandlerOutcome, Input, SoloDeserialized,
    Workers, subscriber,
};
use ruststream_runtime_compio::RustStream;

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
    assert_eq!(described.info().title, "orders");
    assert_eq!(described.servers().keys().collect::<Vec<_>>(), ["main"]);
    assert!(described.handlers().is_empty());
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
