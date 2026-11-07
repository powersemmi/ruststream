//! Conformance test suite that any broker's in-process mode must pass, and the ladder checks
//! every broker must pass.
//!
//! Broker authors prove their in-process transport honours Core routing by running
//! [`run_suite`] against their production broker: each scenario starts from a fresh broker
//! produced by the caller-supplied factory, connects it through its
//! [`InProcess`] transition and drives the connected form through the broker's own
//! [`Subscribe`] / [`TestableBroker::inject`] surface - no server.
//!
//! # Examples
//!
//! The example uses [`ruststream::memory::MemoryBroker`](https://docs.rs/ruststream/latest/ruststream/memory/struct.MemoryBroker.html), so it needs the `memory` feature; a broker
//! crate passes its own production broker here.
//!
//! ```no_run
//! # async fn run() {
//! use ruststream::{conformance::harness, memory::MemoryBroker};
//!
//! harness::run_suite(MemoryBroker::new).await;
//! # }
//! ```

use std::fmt;
use std::time::Duration;

use futures::StreamExt;
#[cfg(doc)]
use ruststream_core::Subscribe;
use ruststream_core::{Headers, OutgoingFor, OutgoingMessage, PayloadForm, Settle};
#[cfg(doc)]
use ruststream_runtime::testing::{InProcess, TestableBroker};
use tokio::time::timeout;

mod local;
mod local_readiness;
mod routing;
#[cfg(ruststream_send_family)]
mod send;

#[cfg(ruststream_send_family)]
pub use super::retry::redelivery_address;
pub use local::{local_lifecycle, local_refused_publish_returns_message, run_local_suite};
pub use local_readiness::local_readiness;
#[cfg(all(ruststream_send_family, feature = "asyncapi"))]
pub use send::describes_without_credentials;
#[cfg(ruststream_send_family)]
pub(crate) use send::on_foreign_runtime;
#[cfg(ruststream_send_family)]
pub use send::{InProcessBroker, lifecycle, run_suite};

#[cfg(ruststream_send_family)]
pub(crate) use routing::unknown_backlog;

/// A message to `name` carrying `payload`, in the form the publisher `P` takes it.
#[cfg(ruststream_alloc)]
pub(crate) fn outgoing<'a, P: PayloadForm>(name: &'a str, payload: &'a [u8]) -> OutgoingFor<'a, P> {
    OutgoingMessage::with_payload(name, payload.into())
}

/// A message to `name` carrying `payload`, in the form the publisher `P` takes it. The fixed form
/// refuses a payload past its room, which a suite's small payloads never are.
#[cfg(not(ruststream_alloc))]
pub(crate) fn outgoing<'a, P: PayloadForm>(name: &'a str, payload: &'a [u8]) -> OutgoingFor<'a, P> {
    let form = <P::Form<'a>>::try_from(payload)
        .unwrap_or_else(|_| panic!("the payload of {name:?} does not fit the publisher's form"));
    OutgoingMessage::with_payload(name, form)
}

/// The message with a map the scenario built: handed over where the message owns its headers.
#[cfg(ruststream_alloc)]
pub(crate) fn with_map<'a, Payload>(
    msg: OutgoingMessage<'a, Payload>,
    headers: &'a Headers,
) -> OutgoingMessage<'a, Payload> {
    msg.with_headers(headers.clone())
}

/// The message with a map the scenario built: lent where the message borrows its headers.
#[cfg(not(ruststream_alloc))]
pub(crate) fn with_map<'a, Payload>(
    msg: OutgoingMessage<'a, Payload>,
    headers: &'a Headers,
) -> OutgoingMessage<'a, Payload> {
    msg.with_headers(headers)
}

/// Sets a header, in either representation of the headers.
pub(crate) fn set_header(headers: &mut Headers, name: &'static str, value: &'static str) {
    #[cfg(ruststream_alloc)]
    let _ = headers.insert(name, value);
    #[cfg(not(ruststream_alloc))]
    headers
        .insert(name, value)
        .unwrap_or_else(|_| panic!("the header {name:?} does not fit the headers of this build"));
}

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);
const NEGATIVE_WAIT: Duration = Duration::from_millis(100);

pub(crate) async fn expect_next<S, M, E>(stream: &mut S, label: &str) -> M
where
    S: futures::Stream<Item = Result<M, E>> + Unpin,
    M: Settle,
    E: fmt::Debug,
{
    let item = timeout(DEFAULT_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| panic!("{label}: stream timed out"));
    let item = item.unwrap_or_else(|| panic!("{label}: stream ended unexpectedly"));
    item.unwrap_or_else(|err| panic!("{label}: stream yielded error: {err:?}"))
}

pub(crate) async fn expect_no_more<S, M, E>(stream: &mut S, label: &str)
where
    S: futures::Stream<Item = Result<M, E>> + Unpin,
    M: Settle,
    E: fmt::Debug,
{
    let result = timeout(NEGATIVE_WAIT, stream.next()).await;
    assert!(
        result.is_err(),
        "{label}: expected no further deliveries within {NEGATIVE_WAIT:?}",
    );
}
