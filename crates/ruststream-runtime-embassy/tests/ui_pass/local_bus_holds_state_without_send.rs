// With the Tokio runner off, nothing is asked to be `Send`: a handler keeps an `Rc` across an
// await, and the app is built around the local bus, whose state is not `Send` either.
use std::rc::Rc;

use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
use ruststream::{Deserialized, subscriber};

#[derive(Deserialized)]
struct Reading<'a>(&'a [u8]);

#[subscriber(LocalMemorySource::new("readings"))]
async fn on_reading(reading: &Reading<'_>) -> HandlerOutcome {
    let held = Rc::new(reading.0.len());
    embassy_futures::yield_now().await;
    let _ = held;
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = embassy)]
fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b.include(on_reading))
}

fn main() {}
