//! The harness starts an app in the service's own order: the `on_startup` state producer first,
//! the brokers after it, the subscriptions, and `after_startup` last.
//!
//! [`Counted`] is a broker that counts its connects, whichever transition makes them, so a test
//! reads how many brokers were connected at any point of the start.

#![cfg(all(
    feature = "testing",
    feature = "memory",
    feature = "json",
    feature = "macros"
))]

use std::error::Error;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ruststream::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryPublish, MemoryPublisher,
};
use ruststream::runtime::{AppInfo, HandlerOutcome, PublishExt, RustStream};
use ruststream::testing::{InProcess, TestApp, TestError};
use ruststream::{Broker, Outgoing, register_testable_broker, subscriber};
use serde::{Deserialize, Serialize};

/// An in-memory broker that counts how many times it was connected.
#[derive(Debug, Clone)]
struct Counted {
    bus: MemoryBroker,
    connects: Arc<AtomicUsize>,
}

impl Counted {
    fn new() -> Self {
        Self {
            bus: MemoryBroker::new(),
            connects: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn connects(&self) -> usize {
        self.connects.load(Ordering::SeqCst)
    }
}

impl Broker for Counted {
    type Error = MemoryError;
    type Connected = ConnectedMemoryBroker;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        self.connects.fetch_add(1, Ordering::SeqCst);
        self.bus.connect()
    }
}

impl InProcess for Counted {
    async fn connect_in_process(self) -> Result<Self::Connected, Self::Error> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        self.bus.connect().await
    }
}

register_testable_broker!(Counted);

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn take_order(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// Why the state producer below refused to start the service.
#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error("the configuration is missing")]
    Missing,
    #[error("{0} broker(s) were already connected when the state was produced")]
    Connected(usize),
}

/// An app whose `on_startup` fails.
fn failing_app(broker: Counted) -> RustStream {
    RustStream::new(AppInfo::new("failing", "0.1.0"))
        .on_startup(async move |()| Err::<(), _>(StartupError::Missing))
        .with_broker(broker, |b| {
            b.include(take_order);
        })
}

/// An app whose `on_startup` refuses to run once a broker is connected, as it never is in
/// production.
fn observing_app(broker: Counted) -> RustStream {
    let observed = broker.clone();
    RustStream::new(AppInfo::new("observing", "0.1.0"))
        .on_startup(async move |()| match observed.connects() {
            0 => Ok(()),
            connected => Err(StartupError::Connected(connected)),
        })
        .with_broker(broker, |b| {
            b.include(take_order);
        })
}

async fn start(app: RustStream, live: bool) -> Result<TestApp<()>, TestError> {
    if live {
        TestApp::start_live(app).await
    } else {
        TestApp::start(app).await
    }
}

/// A failing `on_startup` connects nothing, in either mode, as the service connects nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_on_startup_connects_no_broker() {
    for live in [false, true] {
        let broker = Counted::new();
        match start(failing_app(broker.clone()), live).await {
            Err(TestError::Startup(source)) => assert_eq!(
                source.to_string(),
                "the configuration is missing",
                "the hook's own error is the start's (live: {live})",
            ),
            other => panic!(
                "expected the start to fail (live: {live}), got {:?}",
                other.map(|_| ())
            ),
        }
        assert_eq!(
            broker.connects(),
            0,
            "a failed on_startup left a broker connected (live: {live})",
        );
    }
}

/// `on_startup` runs before any broker connects, in either mode, and the brokers connect after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_startup_runs_before_the_brokers_connect() -> Result<(), Box<dyn Error>> {
    for live in [false, true] {
        let broker = Counted::new();
        let tb = start(observing_app(broker.clone()), live).await?;
        assert_eq!(broker.connects(), 1, "live: {live}");
        tb.broker::<Counted>()
            .message(&Order { id: 1 })
            .publish()
            .await?;
        tb.broker::<Counted>()
            .subscriber("orders")
            .assert_called_once();
        tb.shutdown().await?;
    }
    Ok(())
}

/// Announces itself on the app's own subscription once the service has started.
fn announcing_app() -> RustStream {
    RustStream::new(AppInfo::new("announcing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.after_startup(MemoryPublish, async move |publisher: MemoryPublisher| {
            publisher
                .message(&Order { id: 0 })
                .publish()
                .await
                .map_err(|err| io::Error::other(err.to_string()))
        });
        b.include(take_order);
    })
}

/// `after_startup` runs once the subscriptions are open: the broker keeps nothing published to
/// a name nobody reads, so the announcement reaches the subscription only because it was
/// already there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_startup_runs_once_the_subscriptions_are_open() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(announcing_app()).await?;
    tb.settle().await?;
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 0 });
    tb.shutdown().await?;
    Ok(())
}
