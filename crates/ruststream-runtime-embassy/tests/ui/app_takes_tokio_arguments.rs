// The refused items expand to nothing, so the imports they would use stay unused.
#![allow(unused_imports)]

use ruststream::memory::local::LocalMemoryBroker;
use ruststream::runtime::{EmbeddedApp, Mounts};

#[ruststream::app(runtime = embassy, flavor = "current_thread")]
fn single_threaded(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b)
}

#[ruststream::app(runtime = embassy, worker_threads = 2)]
fn sized(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b)
}

#[ruststream::app(runtime = embassy)]
fn generic<Bus: ruststream::Broker>(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| b)
}

fn main() {}
