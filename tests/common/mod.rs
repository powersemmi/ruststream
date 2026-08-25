//! Shared helpers for the integration tests.
//!
//! What lives here is what a suite needs but is never the subject of its assertions: the
//! stand-in message, the wait primitive, the connected form the publish-log assertions read
//! from. A suite that tests one of these shapes itself declares its own instead - `app_dispatch`
//! keeps an `Order` carrying a second field because decoding it is the point there.
//!
//! Each test binary compiles its own copy of this module and uses what it needs, hence the
//! `dead_code` allowances.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Waits until `cond` holds, yielding between checks, but no longer than `timeout`.
///
/// The yield loop is the sanctioned no-sleep wait: in multi-thread mode the handler runs on
/// another worker and flips the observed state independently, so yielding is enough to let it
/// progress.
#[allow(dead_code)]
pub(crate) async fn wait_for(mut cond: impl FnMut() -> bool, timeout: Duration) {
    let result = tokio::time::timeout(timeout, async {
        while !cond() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(result.is_ok(), "condition not met within {timeout:?}");
}

/// The message the suites drive when the payload is not what they are asserting on: one field,
/// so a decode either happened or did not.
///
/// The `Outgoing` derive rides along wherever the macros feature is on, which lets a suite
/// publish it through the builder. It declares no name: the subject differs per suite, so the
/// call site keeps naming it with `to(..)`.
#[cfg_attr(feature = "macros", derive(ruststream::Outgoing))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct Order {
    pub(crate) id: u32,
}

/// An [`Order`] encoded as the JSON-decoding subscribers expect to receive it.
#[allow(dead_code)]
pub(crate) fn order_bytes(id: u32) -> Vec<u8> {
    serde_json::to_vec(&Order { id }).expect("an order serializes")
}

/// The reply half of a request/reply suite: what a handler answers with when the answer's shape
/// is not what the suite is asserting on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct Receipt {
    pub(crate) id: u32,
}

/// The stand-in message of the suites that forward rather than reply: same role as [`Order`],
/// distinct so a test driving both sides can tell input from output.
#[cfg_attr(feature = "macros", derive(ruststream::Outgoing))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct Event {
    pub(crate) id: u64,
}

/// An [`Event`] encoded for the wire.
#[allow(dead_code)]
pub(crate) fn payload(id: u64) -> Vec<u8> {
    serde_json::to_vec(&Event { id }).expect("an event serializes")
}

/// The request/response pair of the middleware suites, whose subject is what runs around a
/// handler rather than what the handler carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct Req {
    pub(crate) n: u32,
}

/// The answer to a [`Req`]. See its docs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct Resp {
    pub(crate) n: u32,
}

/// The connected form of `broker`, for the assertions that read a broker's publish log.
///
/// Connecting is where a broker's `TestableBroker` surface appears, and for the in-process bus
/// the transition performs no I/O - so an observer costs nothing but says the quiet part: the
/// log belongs to the connection, not to the configuration the app was built from.
#[cfg(feature = "memory")]
#[allow(dead_code)]
pub(crate) async fn connected(
    broker: &ruststream::memory::MemoryBroker,
) -> ruststream::memory::ConnectedMemoryBroker {
    ruststream::Broker::connect(broker.clone())
        .await
        .expect("memory connect is infallible")
}

/// A fresh memory broker with the two handles a suite drives it through: one to publish input,
/// one to read the publish log. The broker itself is returned to be moved into the app.
#[cfg(feature = "memory")]
#[allow(dead_code)]
pub(crate) async fn observed_memory() -> (
    ruststream::memory::MemoryBroker,
    ruststream::memory::MemoryPublisher,
    ruststream::memory::ConnectedMemoryBroker,
) {
    let broker = ruststream::memory::MemoryBroker::new();
    let ingress = broker.publisher();
    let observer = connected(&broker).await;
    (broker, ingress, observer)
}

/// Asserts that exactly one [`Event`] carrying `id` was published to `name`.
#[cfg(all(feature = "memory", feature = "testing"))]
#[allow(dead_code)]
pub(crate) async fn expect_id(
    observer: &ruststream::memory::ConnectedMemoryBroker,
    name: &str,
    id: u64,
) {
    let seen =
        ruststream::testing::expect_published(observer, name, 1, Duration::from_secs(2)).await;
    assert_eq!(seen.len(), 1, "expected one publish on {name}");
    let event: Event = serde_json::from_slice(seen[0].payload()).expect("decodes");
    assert_eq!(event.id, id);
}

/// A service running in the background, stopped by the handle rather than by a signal.
///
/// The suites that drive `run_until` all want the same thing: start the service on its own task,
/// poke it through a publisher, then end the run and see what it returned. Spelling that out
/// takes a `Notify`, a clone of it, a spawn and a join - five lines of scaffolding around one
/// line of intent. A suite whose subject IS the teardown (a drain that must time out, a signal
/// arriving mid-handler) still writes it by hand.
#[allow(dead_code)]
pub(crate) struct BackgroundRun {
    shutdown: std::sync::Arc<tokio::sync::Notify>,
    join: tokio::task::JoinHandle<Result<(), ruststream::runtime::RustStreamError>>,
}

#[allow(dead_code)]
impl BackgroundRun {
    /// Spawns `app` on its own task, running until [`stop`](Self::stop) is called.
    pub(crate) fn spawn<Layers, State, Pipeline, Phase>(
        app: ruststream::runtime::RustStream<Layers, State, Pipeline, Phase>,
    ) -> Self
    where
        Layers: Send + 'static,
        State: Send + Sync + 'static,
        Pipeline: Send + 'static,
        Phase: Send + 'static,
    {
        let shutdown = std::sync::Arc::new(tokio::sync::Notify::new());
        let signal = std::sync::Arc::clone(&shutdown);
        let join = tokio::spawn(app.run_until(async move { signal.notified().await }));
        Self { shutdown, join }
    }

    /// Ends the run and asserts it shut down gracefully.
    pub(crate) async fn stop(self) {
        self.shutdown.notify_one();
        self.join
            .await
            .expect("the run task must not panic")
            .expect("graceful shutdown failed");
    }

    /// Ends the run and hands back what it returned, for the suites asserting on the reason a
    /// service stopped.
    pub(crate) async fn stop_for_result(self) -> Result<(), ruststream::runtime::RustStreamError> {
        self.shutdown.notify_one();
        self.join.await.expect("the run task must not panic")
    }
}
