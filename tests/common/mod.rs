//! Shared helpers for the integration tests.
//!
//! What lives here is what a suite needs but is never the subject of its assertions: the
//! stand-in message, the wait primitive, the connected form the publish-log assertions read
//! from. A suite that tests one of these shapes itself declares its own instead.
//!
//! Each test binary compiles its own copy of this module and uses what it needs, hence the
//! `dead_code` allowances.
//!
//! Registrations are documented by default, so every message type here derives `JsonSchema`:
//! a suite mounting one of them is not asking to be the file that opts documentation out.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Waits until `cond` holds, yielding between checks, but no longer than `timeout`.
///
/// The yield loop is the sanctioned no-sleep wait: in multi-thread mode the handler runs on
/// another worker and flips the observed state independently, so yielding is enough to let it
/// progress.
///
/// A suite whose subject is the application surface does not need this: the
/// [`TestApp`](ruststream::testing::TestApp) harness settles the whole reaction before its
/// injection returns. What is left here serves the one suite that must observe a RUNNING app
/// mid-reaction: the otel suite, which kills the bus under a handler still holding its delivery.
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[allow(dead_code)]
pub(crate) struct Order {
    pub(crate) id: u32,
}

/// The reply half of a request/reply suite: what a handler answers with when the answer's shape
/// is not what the suite is asserting on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[allow(dead_code)]
pub(crate) struct Receipt {
    pub(crate) id: u32,
}

/// The stand-in message of the suites that forward rather than reply: same role as [`Order`],
/// distinct so a test driving both sides can tell input from output.
#[cfg_attr(feature = "macros", derive(ruststream::Outgoing))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[allow(dead_code)]
pub(crate) struct Event {
    pub(crate) id: u64,
}

/// An [`Event`] encoded for the wire.
#[allow(dead_code)]
pub(crate) fn payload(id: u64) -> Vec<u8> {
    serde_json::to_vec(&Event { id }).expect("an event serializes")
}

/// Bytes published as themselves: the named wire a suite injects an arbitrary payload through.
///
/// Publishing is typed, so bytes that are not a model of their own still travel as a declared
/// type; `#[derive(Serialized)]` is what says "these bytes are already the payload", and no
/// codec runs on them. Suites use it for the two payloads that are deliberately not a model:
/// what a raw (`&[u8]` or `Deserialized`) subscriber is meant to receive, and what a decode
/// policy is meant to reject. It declares no name - the subject differs per suite, so the call
/// site keeps naming it with `to(..)`.
#[cfg_attr(
    feature = "macros",
    derive(ruststream::Outgoing, ruststream::Serialized)
)]
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct Wire(pub(crate) Vec<u8>);

#[allow(dead_code)]
impl Wire {
    /// The wire form of `bytes`, for the call sites that hold a slice or a literal.
    pub(crate) fn of(bytes: impl AsRef<[u8]>) -> Self {
        Self(bytes.as_ref().to_vec())
    }
}

/// The request/response pair of the middleware suites, whose subject is what runs around a
/// handler rather than what the handler carries.
///
/// The request half rides the `Outgoing` derive like [`Order`]: a suite drives these middleware
/// through it, so the request is published, and it declares no name because the subject differs
/// per suite.
#[cfg_attr(feature = "macros", derive(ruststream::Outgoing))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[allow(dead_code)]
pub(crate) struct Req {
    pub(crate) n: u32,
}

/// The answer to a [`Req`]. See its docs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[allow(dead_code)]
pub(crate) struct Resp {
    pub(crate) n: u32,
}

/// The connected form of `broker`, for the assertions that read a broker's publish log.
///
/// A broker's `TestableBroker` surface appears on the connected form: the log belongs to the
/// connection, not to the configuration the app was built from.
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
