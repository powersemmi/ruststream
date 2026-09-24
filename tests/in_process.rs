//! The in-process mode of a broker with a server: the harness and the conformance suites connect
//! it through its [`InProcess`] transition and never through `connect`.
//!
//! [`Dialling`] stands for a networked broker. Its `connect` is the network dial and always fails
//! here, as it would in a test with no server; its in-process transition is the in-memory bus.
//! Everything that passes below would therefore fail the moment anything called `connect`.

#![cfg(all(
    feature = "testing",
    feature = "memory",
    feature = "json",
    feature = "macros"
))]

use std::future::{Future, ready};

use ruststream::conformance::harness::{self, InProcessBroker};
use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemorySource, Retaining, Retention};
use ruststream::runtime::{App, AppInfo, HandlerOutcome, RustStream};
use ruststream::testing::{InProcess, TestApp, TestError};
use ruststream::{Broker, Outgoing, nonzero, register_testable_broker, subscriber};
use serde::{Deserialize, Serialize};

/// A broker whose `connect` dials a server this test does not have.
///
/// Its in-process bus keeps a publish log, which the routing suite reads back.
#[derive(Debug)]
struct Dialling {
    bus: MemoryBroker<Retaining>,
}

impl Default for Dialling {
    fn default() -> Self {
        Self {
            bus: MemoryBroker::retaining(Retention::Messages(nonzero!(64))),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum DialError {
    #[error("no server to dial")]
    NoServer,
}

impl Broker for Dialling {
    type Error = DialError;
    type Connected = ConnectedMemoryBroker<Retaining>;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        ready(Err(DialError::NoServer))
    }
}

// --8<-- [start:in_process]
impl InProcess for Dialling {
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        Ok(self
            .bus
            .connect()
            .await
            .expect("the in-memory bus connects without I/O"))
    }
}

register_testable_broker!(Dialling);
// --8<-- [end:in_process]

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn accept(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// The app a service would ship: built on the production broker, exactly as `main` builds it,
/// and returned the way `#[ruststream::app]` recommends.
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(Dialling::default(), |b| {
        b.include(accept);
    })
}

// The production app starts under the harness although its broker cannot connect, and the test
// addresses that broker by the production type.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_harness_runs_the_production_app_through_the_in_process_transition() {
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<Dialling>()
        .message(&Order { id: 1 })
        .publish()
        .await
        .expect("publish");

    tb.broker::<Dialling>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("shutdown");
}

// --8<-- [start:suites]
// The routing suite connects the production broker in process.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_routing_suite_connects_the_broker_in_process() {
    harness::run_suite(Dialling::default).await;
}

// The suites that connect with `connect` run in process through the adapter, over the production
// broker's own descriptor and publisher.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ladder_suite_runs_in_process_through_the_adapter() {
    harness::lifecycle(
        || InProcessBroker::new(Dialling::default()),
        |name| MemorySource::new(name),
        |connected| connected.publisher(),
    )
    .await;
}
// --8<-- [end:suites]

/// A broker whose in-process transition refuses its configuration.
#[derive(Debug, Default)]
struct Misconfigured;

impl Broker for Misconfigured {
    type Error = DialError;
    type Connected = ConnectedMemoryBroker;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        ready(Err(DialError::NoServer))
    }
}

impl InProcess for Misconfigured {
    fn connect_in_process(
        self,
    ) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        ready(Err(DialError::NoServer))
    }
}

register_testable_broker!(Misconfigured);

// A transition that fails stops the start and names the broker, the way a failed `connect` stops
// a service.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_in_process_transition_is_reported_with_the_broker() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).register_broker(Misconfigured);

    match TestApp::start(app).await {
        Err(TestError::Connect { broker, source }) => {
            assert!(broker.ends_with("Misconfigured"), "{broker}");
            assert_eq!(source.to_string(), "no server to dial");
        }
        other => panic!("expected a failed transition, got {:?}", other.map(|_| ())),
    }
}
