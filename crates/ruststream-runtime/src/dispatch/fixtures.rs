//! The test doubles the dispatch loop's tests and the settle tests share.

use std::future::ready;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use bytes::Bytes;

use crate::{AckError, HeaderMap, IncomingMessage, Lend, OutgoingMessage, Publisher};

/// What a test delivery's transport does when asked to settle. The three cases differ in kind,
/// not degree, so they are variants rather than a flag plus an error slot.
#[derive(Clone, Copy)]
pub(super) enum Settlement {
    /// Settlement lands, as on a broker with acknowledgements.
    Accepted,
    /// The transport has no settlement at all (MQTT `QoS` 0, `ZeroMQ`, Redis pub/sub).
    Unsupported,
    /// The transport does settle, but the broker rejected this one.
    Rejected,
    /// The broker's settlement panics: a fault of the transport, past the handler's guard.
    Panics,
}

impl Settlement {
    fn apply(self) -> Result<(), AckError> {
        match self {
            Self::Accepted => Ok(()),
            Self::Unsupported => Err(AckError::Unsupported),
            Self::Rejected => Err(AckError::Timeout),
            Self::Panics => panic!("the broker's settlement panicked"),
        }
    }
}

/// A delivery without native delayed redelivery: `supports_nack_after` stays at the trait
/// default (`false`), and the default `nack_after` would error. It records how it was settled
/// so a test can assert the fallback dropped it rather than calling `nack(true)`.
pub(super) struct PlainMessage {
    pub(super) payload: Bytes,
    pub(super) headers: HeaderMap,
    // 0 = unset, 1 = nack(false) (dropped), 2 = nack(true) (requeued).
    pub(super) settled: Arc<AtomicU8>,
    pub(super) settlement: Settlement,
}

impl IncomingMessage for PlainMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        ready(self.settlement.apply())
    }

    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        self.settled
            .store(if requeue { 2 } else { 1 }, Ordering::SeqCst);
        ready(self.settlement.apply())
    }
}

/// A publisher that always rejects, standing in for a broker that died between the nack and
/// the deferred republish.
pub(super) struct RejectingPublisher;

impl Publisher for RejectingPublisher {
    type Payload = Lend;
    type Error = io::Error;
    type Options = ();

    fn publish(
        &self,
        _msg: OutgoingMessage<'_, &[u8]>,
        _options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Err(io::Error::other("connection closed")))
    }
}
