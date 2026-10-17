//! The harness shuts an app down in the service's own order: the `on_shutdown` hooks while the
//! brokers are still connected, then the subscriptions and the brokers, and `after_shutdown` last.
//!
//! The hooks write to a [`Journal`], the app's state; the test keeps a clone of it and reads what
//! the hooks wrote once the harness has shut down.

#![cfg(all(
    feature = "testing",
    feature = "memory",
    feature = "json",
    feature = "macros"
))]

use std::error::Error;
use std::io;
use std::sync::{Arc, Mutex, PoisonError};

use ruststream::memory::prelude::*;
use ruststream::runtime::Bound;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq)]
#[outgoing(name = "farewells")]
struct Farewell {
    hook: String,
}

#[subscriber("orders")]
async fn take_order(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// The app's state: what each shutdown hook saw, and a publisher token that tells whether the
/// broker was still connected when the hook ran.
#[derive(Debug, Clone)]
struct Journal {
    entries: Arc<Mutex<Vec<String>>>,
    farewells: Bound<MemoryBroker, Publish>,
}

impl Journal {
    /// Publishes a farewell for `hook` and records whether the broker took it.
    async fn note(&self, hook: &str) {
        let outcome = match self.farewell(hook).await {
            Ok(()) => "broker connected",
            Err(_) => "broker shut down",
        };
        self.record(format!("{hook}: {outcome}"));
    }

    async fn farewell(&self, hook: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let publisher = self.farewells.clone().live().await?;
        publisher
            .message(&Farewell {
                hook: hook.to_owned(),
            })
            .publish()
            .await?;
        Ok(())
    }

    fn record(&self, entry: String) {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(entry);
    }

    fn entries(&self) -> Vec<String> {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// An app whose shutdown hooks write to the journal; `on_shutdown` fails after writing when
/// `failing` is set.
fn journaled_app(failing: bool) -> (impl App<State = Journal>, Journal) {
    let broker = MemoryBroker::new().bindable();
    let journal = Journal {
        entries: Arc::new(Mutex::new(Vec::new())),
        farewells: broker.bind(Publish),
    };
    let state = journal.clone();
    let app = RustStream::new(AppInfo::new("journaled", "0.1.0"))
        .on_startup(async move |()| Ok::<_, io::Error>(state))
        .on_shutdown(async move |journal: Arc<Journal>| {
            journal.note("on_shutdown").await;
            if failing {
                return Err(io::Error::other("flush refused"));
            }
            Ok(())
        })
        .after_shutdown(async move |journal: Arc<Journal>| {
            journal.note("after_shutdown").await;
            Ok::<_, io::Error>(())
        })
        .with_broker(broker, |b| {
            b.include(take_order);
        });
    (app, journal)
}

/// Each shutdown hook runs once, in the service's order, with the state the app produced:
/// `on_shutdown` while the broker is connected, `after_shutdown` once it has shut down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_runs_each_hook_once_in_the_service_order() -> Result<(), Box<dyn Error>> {
    let (app, journal) = journaled_app(false);
    let tb = TestApp::start(app).await?;
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 1 })
        .publish()
        .await?;
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once();
    assert!(
        journal.entries().is_empty(),
        "a shutdown hook ran before shutdown"
    );

    tb.shutdown().await?;

    assert_eq!(
        journal.entries(),
        [
            "on_shutdown: broker connected",
            "after_shutdown: broker shut down"
        ],
    );
    Ok(())
}

/// A failing `on_shutdown` does not stop the shutdown, as it does not stop the service's: the
/// brokers still shut down and `after_shutdown` still runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_on_shutdown_does_not_stop_the_shutdown() -> Result<(), Box<dyn Error>> {
    let (app, journal) = journaled_app(true);
    let tb = TestApp::start(app).await?;

    tb.shutdown().await?;

    assert_eq!(
        journal.entries(),
        [
            "on_shutdown: broker connected",
            "after_shutdown: broker shut down"
        ],
    );
    Ok(())
}
