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
